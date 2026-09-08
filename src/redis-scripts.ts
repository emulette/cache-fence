/** Initialize missing metadata with a new token, or rotate it unconditionally. */
export const GENERATION_SCRIPT = `
local current = redis.call('GET', KEYS[1])
if current == false or ARGV[2] == 'rotate' then
  redis.call('SET', KEYS[1], ARGV[1])
  return ARGV[1]
end
return current
`;

/** Missing metadata rejects the write; only readers may initialize a generation. */
export const FENCED_SET_SCRIPT = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[2], ARGV[1] .. '\\n' .. ARGV[2], 'PX', ARGV[3])
return 1
`;

/** Capture the generation and verify the entry's stamp in one atomic round trip. */
export const READ_SCRIPT = `
local generation = redis.call('GET', KEYS[1])
if generation == false then
  generation = ARGV[1]
  redis.call('SET', KEYS[1], generation)
end
local prefix = generation .. '\\n'
local fresh = redis.pcall('GET', KEYS[2])
if type(fresh) == 'table' then
  return {generation, 'freshError', fresh.err}
end
if fresh ~= false and string.sub(fresh, 1, #prefix) == prefix then
  return {generation, 'fresh', string.sub(fresh, #prefix + 1)}
end
if #KEYS == 3 then
  local stale = redis.pcall('GET', KEYS[3])
  if type(stale) == 'table' then
    return {generation, 'staleError', stale.err}
  end
  if stale ~= false and string.sub(stale, 1, #prefix) == prefix then
    return {generation, 'stale', string.sub(stale, #prefix + 1)}
  end
end
return {generation, 'miss', ''}
`;

/** Check and unlink atomically, preserving writes from the current generation. */
export const SWEEP_SCRIPT = `
local generation = redis.call('GET', KEYS[1])
local prefix = generation and (generation .. '\\n')
local deleted = 0
for i = 2, #KEYS do
  local stamp = redis.pcall('GETRANGE', KEYS[i], 0, 36)
  if type(stamp) == 'table' or not prefix or stamp ~= prefix then
    deleted = deleted + redis.call('UNLINK', KEYS[i])
  end
end
return deleted
`;
