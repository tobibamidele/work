package redis

import goredis "github.com/redis/go-redis/v9"

// All state changes are Lua scripts. Redis runs a script atomically, so each
// script is one linearizable step: claiming, acknowledging, cancelling and
// enqueueing can never interleave. Every script receives the key prefix as
// ARGV[1] and returns {"OK", ...} or {"ERR", code, ...}.
//
// Data model (P is "{prefix}", a hash tag so every key lives in one cluster
// slot):
//
//	P:rec|<id>          hash, the record (timestamps are int64 microseconds, 0 = unset)
//	P:all               zset  id by created_at   (inspection, newest first)
//	P:s|<status>        zset  id by created_at   (inspection by status)
//	P:r|<queue>|<type>  zset  READY work. Every score is 0, so members sort
//	                    lexicographically: a fixed-width key of
//	                    (inverted priority, available_at, created_at) followed
//	                    by the id. That yields exactly "priority DESC,
//	                    available_at ASC, created_at ASC, id ASC".
//	P:d|<queue>|<type>  zset  DELAYED work, id by available_at
//	P:leases            zset  running id by lease_until
//	P:u|<queue>|<key>   string id of the active work holding a unique key
//	P:t|<queue>         set   work types seen in a queue
//	P:stats             hash  "<queue>|<status>" -> count
const luaPrelude = `
local P = ARGV[1]
local LEASES = P .. ':leases'
local STATS = P .. ':stats'
local ALL = P .. ':all'
local function rk(id) return P .. ':rec|' .. id end
local function sk(s) return P .. ':s|' .. s end
local function rdy(q, t) return P .. ':r|' .. q .. '|' .. t end
local function dly(q, t) return P .. ':d|' .. q .. '|' .. t end
local function typesk(q) return P .. ':t|' .. q end
local function uk(q, k) return P .. ':u|' .. q .. '|' .. k end
local function int(v) return string.format('%d', v) end
local function num(v) return tonumber(v) or 0 end

local function load(id)
  local flat = redis.call('HGETALL', rk(id))
  if #flat == 0 then return nil end
  local h = {}
  for i = 1, #flat, 2 do h[flat[i]] = flat[i + 1] end
  return h
end

-- member is the sort key of ready work: inverted priority, available_at,
-- created_at (all zero padded, fixed width) then the id.
local function member(h, id)
  local inv = 4294967295 - (num(h.priority) + 2147483648)
  return string.format('%010d%016d%016d', inv, num(h.available_at), num(h.created_at)) .. id
end

local function move(q, id, created, old, new)
  if old ~= '' then
    redis.call('HINCRBY', STATS, q .. '|' .. old, -1)
    redis.call('ZREM', sk(old), id)
  end
  if new ~= '' then
    redis.call('HINCRBY', STATS, q .. '|' .. new, 1)
    redis.call('ZADD', sk(new), created, id)
  end
end

-- place makes an available record claimable now (ready) or later (delayed).
local function place(h, id, now)
  if num(h.available_at) <= now then
    redis.call('ZADD', rdy(h.queue, h.type), 0, member(h, id))
  else
    redis.call('ZADD', dly(h.queue, h.type), num(h.available_at), id)
  end
end

local function unplace(h, id)
  redis.call('ZREM', rdy(h.queue, h.type), member(h, id))
  redis.call('ZREM', dly(h.queue, h.type), id)
end

-- release_unique frees the unique key held by an active record.
local function release_unique(h, id)
  if h.active_unique_key ~= nil and h.active_unique_key ~= '' then
    local k = uk(h.queue, h.active_unique_key)
    if redis.call('GET', k) == id then redis.call('DEL', k) end
    redis.call('HSET', rk(id), 'active_unique_key', '')
  end
end

local function owned(id, worker)
  local h = load(id)
  if not h then return nil, 'NOTFOUND' end
  if h.status == 'running' and h.worker_id == worker then return h end
  if h.status == 'completed' then return nil, 'COMPLETED' end
  if h.status == 'cancelled' then return nil, 'CANCELLED' end
  return nil, 'LOST'
end

local function clear_lease(id)
  redis.call('ZREM', LEASES, id)
  redis.call('HSET', rk(id), 'worker_id', '', 'locked_at', '0', 'lease_until', '0')
end
`

var (
	enqueueScript = goredis.NewScript(luaPrelude + `
local f = {}
for i = 2, #ARGV, 2 do f[ARGV[i]] = ARGV[i + 1] end
local id = f.id
if redis.call('EXISTS', rk(id)) == 1 then return {'ERR', 'EXISTS'} end
if f.active_unique_key ~= '' then
  local k = uk(f.queue, f.active_unique_key)
  if not redis.call('SET', k, id, 'NX') then
    return {'ERR', 'DUP', redis.call('GET', k)}
  end
end
local args = {}
for i = 2, #ARGV do args[#args + 1] = ARGV[i] end
redis.call('HSET', rk(id), unpack(args))
redis.call('ZADD', ALL, num(f.created_at), id)
redis.call('SADD', typesk(f.queue), f.type)
move(f.queue, id, f.created_at, '', f.status)
if f.status == 'available' then
  place(f, id, num(f.updated_at))
elseif f.status == 'running' and num(f.lease_until) ~= 0 then
  redis.call('ZADD', LEASES, num(f.lease_until), id)
end
return {'OK'}
`)

	claimScript = goredis.NewScript(luaPrelude + `
local now = num(ARGV[2])
local leaseUs = num(ARGV[3])
local worker = ARGV[4]
local limit = num(ARGV[5])
local nq = num(ARGV[6])
local queues, qset = {}, {}
for i = 1, nq do queues[i] = ARGV[6 + i]; qset[ARGV[6 + i]] = true end
local nt = num(ARGV[7 + nq])
local types, tset = {}, nil
if nt > 0 then
  tset = {}
  for i = 1, nt do types[i] = ARGV[7 + nq + i]; tset[types[i]] = true end
end

local combos = {}
for _, q in ipairs(queues) do
  local ts = types
  if nt == 0 then ts = redis.call('SMEMBERS', typesk(q)) end
  for _, t in ipairs(ts) do combos[#combos + 1] = {q, t} end
end

-- 1. delayed work that is due becomes ready
for _, c in ipairs(combos) do
  local due = redis.call('ZRANGEBYSCORE', dly(c[1], c[2]), '-inf', ARGV[2], 'LIMIT', 0, 1000)
  for _, id in ipairs(due) do
    local h = load(id)
    redis.call('ZREM', dly(c[1], c[2]), id)
    if h and h.status == 'available' then
      redis.call('ZADD', rdy(c[1], c[2]), 0, member(h, id))
    end
  end
end

-- 2. candidates: the head of every ready set, plus running work whose lease
-- expired (and that may run again), merged in claim order
local cands = {}
for _, c in ipairs(combos) do
  local key = rdy(c[1], c[2])
  for _, m in ipairs(redis.call('ZRANGE', key, 0, limit - 1)) do
    cands[#cands + 1] = {key = m, id = string.sub(m, 43), src = 'r', zkey = key}
  end
end
for _, id in ipairs(redis.call('ZRANGEBYSCORE', LEASES, '-inf', '(' .. ARGV[2], 'LIMIT', 0, 500)) do
  local h = load(id)
  if h and h.status == 'running' and qset[h.queue] and (tset == nil or tset[h.type])
     and num(h.attempts) < num(h.max_attempts) and num(h.cancel_requested_at) == 0 then
    cands[#cands + 1] = {key = member(h, id), id = id, src = 'l'}
  end
end
table.sort(cands, function(a, b) return a.key < b.key end)

-- 3. claim the first <limit>
local out = {}
for i = 1, math.min(limit, #cands) do
  local c = cands[i]
  local h = load(c.id)
  if c.src == 'r' then redis.call('ZREM', c.zkey, c.key) end
  if h and ((c.src == 'r' and h.status == 'available') or (c.src == 'l' and h.status == 'running')) then
    local old = h.status
    local sets = {
      'status', 'running', 'attempts', int(num(h.attempts) + 1), 'worker_id', worker,
      'locked_at', ARGV[2], 'lease_until', int(now + leaseUs), 'started_at', ARGV[2], 'updated_at', ARGV[2],
    }
    if old == 'running' then
      sets[#sets + 1] = 'last_error'; sets[#sets + 1] = 'lease expired; reclaimed by another worker'
      sets[#sets + 1] = 'last_error_at'; sets[#sets + 1] = ARGV[2]
    end
    redis.call('HSET', rk(c.id), unpack(sets))
    redis.call('ZADD', LEASES, now + leaseUs, c.id)
    if old == 'available' then move(h.queue, c.id, h.created_at, 'available', 'running') end
    out[#out + 1] = redis.call('HGETALL', rk(c.id))
  end
end
return out
`)

	completeScript = goredis.NewScript(luaPrelude + `
local id, worker, now = ARGV[2], ARGV[3], ARGV[4]
local h, e = owned(id, worker)
if not h then return {'ERR', e} end
clear_lease(id)
release_unique(h, id)
redis.call('HSET', rk(id), 'status', 'completed', 'completed_at', now, 'updated_at', now, 'cancel_requested_at', '0')
move(h.queue, id, h.created_at, 'running', 'completed')
return {'OK'}
`)

	retryScript = goredis.NewScript(luaPrelude + `
local id, worker, nextAt, now, err = ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]
local h, e = owned(id, worker)
if not h then return {'ERR', e} end
clear_lease(id)
redis.call('HSET', rk(id), 'last_error', err, 'last_error_at', now, 'updated_at', now)
if num(h.cancel_requested_at) ~= 0 then
  release_unique(h, id)
  redis.call('HSET', rk(id), 'status', 'cancelled')
  move(h.queue, id, h.created_at, 'running', 'cancelled')
else
  redis.call('HSET', rk(id), 'status', 'available', 'available_at', nextAt)
  h.available_at = nextAt
  place(h, id, num(now))
  move(h.queue, id, h.created_at, 'running', 'available')
end
return {'OK'}
`)

	failScript = goredis.NewScript(luaPrelude + `
local id, worker, now, err = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local h, e = owned(id, worker)
if not h then return {'ERR', e} end
clear_lease(id)
release_unique(h, id)
redis.call('HSET', rk(id), 'status', 'failed', 'last_error', err, 'last_error_at', now,
  'updated_at', now, 'cancel_requested_at', '0')
move(h.queue, id, h.created_at, 'running', 'failed')
return {'OK'}
`)

	releaseScript = goredis.NewScript(luaPrelude + `
local id, worker, now = ARGV[2], ARGV[3], ARGV[4]
local h, e = owned(id, worker)
if not h then return {'ERR', e} end
local attempts = num(h.attempts)
if attempts > 0 then attempts = attempts - 1 end
clear_lease(id)
redis.call('HSET', rk(id), 'attempts', int(attempts), 'updated_at', now)
if num(h.cancel_requested_at) ~= 0 then
  release_unique(h, id)
  redis.call('HSET', rk(id), 'status', 'cancelled')
  move(h.queue, id, h.created_at, 'running', 'cancelled')
else
  redis.call('HSET', rk(id), 'status', 'available', 'available_at', now)
  h.available_at = now
  place(h, id, num(now))
  move(h.queue, id, h.created_at, 'running', 'available')
end
return {'OK'}
`)

	finishCancelScript = goredis.NewScript(luaPrelude + `
local id, worker, now, hasCause, err = ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]
local h, e = owned(id, worker)
if not h then return {'ERR', e} end
clear_lease(id)
release_unique(h, id)
redis.call('HSET', rk(id), 'status', 'cancelled', 'updated_at', now)
if hasCause == '1' then redis.call('HSET', rk(id), 'last_error', err, 'last_error_at', now) end
move(h.queue, id, h.created_at, 'running', 'cancelled')
return {'OK'}
`)

	extendScript = goredis.NewScript(luaPrelude + `
local id, worker, untilAt, now = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local h, e = owned(id, worker)
if not h then return {'ERR', e} end
redis.call('HSET', rk(id), 'lease_until', untilAt, 'updated_at', now)
redis.call('ZADD', LEASES, num(untilAt), id)
if num(h.cancel_requested_at) ~= 0 then return {'OK', 1} end
return {'OK', 0}
`)

	cancelScript = goredis.NewScript(luaPrelude + `
local id, now = ARGV[2], ARGV[3]
local h = load(id)
if not h then return {'ERR', 'NOTFOUND'} end
if h.status == 'available' then
  unplace(h, id)
  release_unique(h, id)
  redis.call('HSET', rk(id), 'status', 'cancelled', 'updated_at', now)
  move(h.queue, id, h.created_at, 'available', 'cancelled')
  return {'OK', 'CANCELLED'}
elseif h.status == 'running' then
  if num(h.cancel_requested_at) == 0 then
    redis.call('HSET', rk(id), 'cancel_requested_at', now, 'updated_at', now)
  end
  return {'OK', 'REQUESTED'}
elseif h.status == 'cancelled' then
  return {'OK', 'CANCELLED'}
elseif h.status == 'completed' then
  return {'ERR', 'COMPLETED'}
end
return {'ERR', 'INVALID', h.status}
`)

	requeueScript = goredis.NewScript(luaPrelude + `
local id, now = ARGV[2], ARGV[3]
local h = load(id)
if not h then return {'ERR', 'NOTFOUND'} end
if h.status ~= 'failed' and h.status ~= 'cancelled' then return {'ERR', 'INVALID', h.status} end
if h.unique_key ~= nil and h.unique_key ~= '' then
  local k = uk(h.queue, h.unique_key)
  if not redis.call('SET', k, id, 'NX') then return {'ERR', 'DUP', redis.call('GET', k)} end
  redis.call('HSET', rk(id), 'active_unique_key', h.unique_key)
end
redis.call('HSET', rk(id), 'status', 'available', 'attempts', '0', 'available_at', now, 'updated_at', now,
  'completed_at', '0', 'cancel_requested_at', '0', 'worker_id', '', 'locked_at', '0', 'lease_until', '0')
h.available_at = now
place(h, id, num(now))
move(h.queue, id, h.created_at, h.status, 'available')
return {'OK'}
`)

	reapScript = goredis.NewScript(luaPrelude + `
local now, limit = ARGV[2], num(ARGV[3])
local out = {}
for _, id in ipairs(redis.call('ZRANGEBYSCORE', LEASES, '-inf', '(' .. now, 'LIMIT', 0, 1000)) do
  if #out >= limit then break end
  local h = load(id)
  if not h then
    redis.call('ZREM', LEASES, id)
  elseif h.status == 'running' and (num(h.attempts) >= num(h.max_attempts) or num(h.cancel_requested_at) ~= 0) then
    local ns, msg = 'failed', 'lease expired and no attempts remain'
    if num(h.cancel_requested_at) ~= 0 then
      ns, msg = 'cancelled', 'lease expired after cancellation was requested'
    end
    clear_lease(id)
    release_unique(h, id)
    redis.call('HSET', rk(id), 'status', ns, 'last_error', msg, 'last_error_at', now, 'updated_at', now)
    move(h.queue, id, h.created_at, 'running', ns)
    out[#out + 1] = redis.call('HGETALL', rk(id))
  end
end
return out
`)

	pruneScript = goredis.NewScript(luaPrelude + `
local status, before, limit = ARGV[2], ARGV[3], num(ARGV[4])
local n = 0
for _, id in ipairs(redis.call('ZRANGEBYSCORE', sk(status), '-inf', '(' .. before, 'LIMIT', 0, limit)) do
  local h = load(id)
  if h and h.status == status then
    redis.call('DEL', rk(id))
    redis.call('ZREM', ALL, id)
    move(h.queue, id, h.created_at, status, '')
    n = n + 1
  else
    redis.call('ZREM', sk(status), id)
  end
end
return {'OK', n}
`)
)
