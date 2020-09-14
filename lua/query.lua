-- Need a Math equivalent of this which can be optimized.
local max_key = KEYS[1]
local min_key = KEYS[2]
local query_start = tonumber(ARGV[1])
local query_end = tonumber(ARGV[2])
local result = {}

local minima = redis.call("ZRANGEBYSCORE",min_key,"-inf",query_end,"WITHSCORES")
local maxima = redis.call("ZRANGEBYSCORE",max_key,query_start,"+inf","WITHSCORES")

for min_ctr=1,table.getn(minima),2 do 
    for max_ctr=1,table.getn(maxima),2 do 
        if minima[min_ctr] == maxima[max_ctr] then table.insert(result, {minima[min_ctr],minima[min_ctr+1],maxima[max_ctr+1]}) end
    end
end

return result
--Returns Sorted set of Table Names and Ranges
--[N][0] = TableName
--[N][1] = Start
--[N][3] = End

-- Example
-- ZADD MAX 100 1
-- ZADD MAX 90 2
-- ZADD MAX 50 3
-- ZADD MAX 50 4
-- ZADD MAX 90 5
-- ZADD MAX 20 6

-- ZADD MIN 0 1
-- ZADD MIN 50 2
-- ZADD MIN 40 3
-- ZADD MIN 10 4
-- ZADD MIN 70 5
-- ZADD MIN 10 6
--redis-cli --eval /Users/laukikragji/Documents/Git/Local/partition-pg/lua/query.lua MAX MIN , 30 60