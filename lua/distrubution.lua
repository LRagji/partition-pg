------------------------------------Functions------------------------------------
local ParseDBDef = function (inputstr)
    local sep=","
    local t={}
    for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
            table.insert(t, tonumber(str))
    end
    return t
end

local FetchAndSetNewDB = function(all_db_key,current_db_key,current_db_maximum_tables_key,current_db_table_maximum_rows_key)
    local next_db = redis.call("ZPOPMIN",all_db_key,1)
    if table.getn(next_db) == 0 then return {-1,"No more databases available"} end --When no more Db are available
    local new_DB_Def = ParseDBDef(next_db[1])
    redis.call("SET",current_db_key,new_DB_Def[1])
    redis.call("SET",current_db_maximum_tables_key,new_DB_Def[2])
    redis.call("SET",current_db_table_maximum_rows_key,new_DB_Def[3])
    return {0}
    --Retuns
    --[0]: -1 = No more space or ERROR; 0 = Sucess
end 

local AcquireTable = function (all_db_key, current_db_key, current_db_maximum_tables_key, current_db_table_maximum_rows_key)
    local current_db = redis.call("GET",current_db_key)
    if current_db == false then --When Key is not existing start up scenario
        local result = FetchAndSetNewDB(all_db_key, current_db_key, current_db_maximum_tables_key, current_db_table_maximum_rows_key)
        if(result[1] ~= 0) then return result end
    end

    local acquired_table = redis.call("DECR",current_db_maximum_tables_key)

    if acquired_table <= -1 then
        local result = FetchAndSetNewDB(all_db_key, current_db_key, current_db_maximum_tables_key, current_db_table_maximum_rows_key)
        if(result[1] ~= 0) then return result end
        acquired_table = redis.call("DECR",current_db_maximum_tables_key)
    end 

    return {0, tonumber(redis.call("GET",current_db_key)),acquired_table+1,tonumber(redis.call("GET",current_db_table_maximum_rows_key))}
    --Retuns
    --[0]: -1 = No more space or ERROR; 0 = Sucess
    --[1]: DB ID
    --[2]: Table ID
    --[3]: Maximum Rows
end

local CreateType = function(type_id,all_db_key, current_db_key, current_db_maximum_tables_key, current_db_table_maximum_rows_key)
    local assigned_table = AcquireTable(all_db_key,current_db_key,current_db_maximum_tables_key,current_db_table_maximum_rows_key);
    if(assigned_table[1] ~= 0) then return assigned_table end
    redis.call("HMSET",type_id,"D",assigned_table[2])
    redis.call("HMSET",type_id,"T",assigned_table[3])
    redis.call("HMSET",type_id,"R",assigned_table[4])
    return {0}
    --Retuns
    --[0]: -1 = No more space or ERROR; 0 = Sucess
end

local AcquireRow = function(type_key,all_db_key,current_db_key,current_db_maximum_tables_key,current_db_table_maximum_rows_key)
    local acquired_row = redis.call("HINCRBY",type_key,"R",-1)
    local row_state = redis.call("HMGET",type_key,"D","T","R")
    row_state[1] = tonumber(row_state[1])
    row_state[2] = tonumber(row_state[2])
    row_state[3] = row_state[3]+1
    table.insert(row_state, 1, 0)

    if acquired_row <= -1 then
        redis.call("DEL",type_key)
        local result = CreateType(type_key,all_db_key,current_db_key,current_db_maximum_tables_key,current_db_table_maximum_rows_key)
        if(result[1] ~= 0) then return result end
    end 

    --Retuns
    --[0]: -1 = No more space or ERROR; 0 = Sucess
    --[1]: DB ID
    --[2]: Table ID
    --[3]: Maximum Rows
    return row_state
end

------------------------------------Main------------------------------------
local type_key = KEYS[1]
local all_db_key = KEYS[2]
local current_db_key = KEYS[3]
local current_db_maximum_tables_key = KEYS[4]
local current_db_table_maximum_rows_key = KEYS[5]
local range = tonumber(ARGV[1])

local type_exists = redis.call("EXISTS",type_key)
if type_exists == 0 then 
    local result = CreateType(type_key,all_db_key,current_db_key,current_db_maximum_tables_key,current_db_table_maximum_rows_key)
    if(result[1] ~= 0) then return result end
end 

local DB = 0
local T = 0
local R = 0
local finalResult = {}
local lastResult = nil
while(range>0)--This may be the most inefficent method, Maths should make this blazing fast but pushed for later
do
    local result = AcquireRow(type_key,all_db_key,current_db_key,current_db_maximum_tables_key,current_db_table_maximum_rows_key)
    if(result[1] ~= 0) then 
        if table.getn(finalResult) == 0 then return result end
        table.insert(finalResult, 1, {result[1],result[2]})
        table.insert(finalResult, 1, -2)-- Partial Success
        return finalResult
    else
        ---local lastIdx = table.maxn(finalResult)
        if lastResult == nil or range == 1 then 
            lastResult = {range,result[2],result[3],result[4]}
            table.insert(finalResult, 1, lastResult)
        elseif lastResult[2] ~= result[2] or lastResult[3] ~= result[3] then 
            lastResult = {range,result[2],result[3],result[4]}
            table.insert(finalResult, 1, lastResult)
        end
    end
   range = range-1
end
table.insert(finalResult, 1, 0)
return  finalResult
--Retuns
--[0]: -1 = No more space or ERROR; 0 = Sucess; -2 Partial success
--[N][0]: counter
--[N][1]: DB ID
--[N][2]: Table ID
--[N][3]: Rows

--redis-cli --eval /Users/laukikragji/Documents/Git/Local/partition-pg/lua/acquire-table.lua Type1 DBS cdb mt mr
--SCRIPT LOAD "$(cat /Users/laukikragji/Documents/Git/Local/partition-pg/lua/acquire-table.lua)"

