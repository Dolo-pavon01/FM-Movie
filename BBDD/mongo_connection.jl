import Mongoc: Client, BSON, as_dict, find, find_one, insert_one, insert_many, aggregate, find_and_modify
import Base.Threads.@threads

struct Mongo
    connection::Client
end

function Mongo(host::String, port::Int)::Mongo
    try
        mongo_connection = Client(host, port)
        return Mongo(mongo_connection)
    catch e
        println("Error while creating Mongo instance.")
        rethrow(e)
    end
end

function find_content(mongo::Mongo, database::String, collection::String, query::Dict=Dict(), projection::Dict=Dict(), sort::Dict=Dict())::Dict
    try
        mongo_collection = mongo.connection[database][collection]
        query_bson = BSON(query)
        options_dict = Dict("projection" => projection, "sort" => sort)
        options_bson = BSON(options_dict)
        result = find_one(mongo_collection, query_bson, options=options_bson)
        if !isnothing(result)
            result = as_dict(result)
        end
        return result
    catch e
        println("Error while getting element: $e")
        result = nothing
    end
    return result
end

function find_many_contents(mongo::Mongo, database::String, collection::String; query::Dict=Dict(), projection::Dict=Dict(), sort::Dict=Dict(), limit::Int64=0)::Array
    try
        mongo_collection = mongo.connection[database][collection]
        query_bson = BSON(query)
        options_dict = Dict("projection" => projection, "sort" => sort, "limit" => limit)
        options_bson = BSON(options_dict)
        result = find(mongo_collection, query_bson, options=options_bson)
        if !isnothing(result)
            result_array = Dict[]
            for item in result
                push!(result_array, as_dict(item))
            end
            result = result_array
        end
        return result
    catch e
        println("Error while getting many elements: $e")
        result = nothing
    end
    return result
end

function insert_content(mongo::Mongo, database::String, collection::String, content::Dict)
    try
        mongo_collection = mongo.connection[database][collection]
        bson_content = BSON(content)
        insert_one(mongo_collection, bson_content)
    catch e
        rethrow("Error inserting content: $e")
    end
end


function insert_contents_without_connection(contents, host, port, database, collection)
    mongo = Mongo(host, port)
    mongo_collection = mongo.connection[database][collection]
    documents = BSON[]
    for content in contents
        push!(documents, BSON(content))
    end
    insert_many(mongo_collection, documents)
    return documents
end

function insert_many_contents(mongo::Mongo, database::String, collection::String, contents::Array)
    try
        mongo_collection = mongo.connection[database][collection]
        documents = BSON[]
        for content in contents
            push!(documents, BSON(content))
        end
        insert_many(mongo_collection, documents)
    catch e
        rethrow("Error inserting content: $e")
    end
end

function insert_many_contents(host::String, port::Int, database::String, collection::String, contents::Array)
    try
        
        chunks = Iterators.partition(contents, length(contents) ÷ Threads.nthreads())
        tasks = map(chunks) do chunk
            Threads.@spawn insert_contents_without_connection(chunk, host, port, database, collection)
        end
        chunk_sums = fetch.(tasks)
        return chunk_sums


    catch e
        rethrow("Error inserting content: $e")
    end
end

function run_aggregate(mongo::Mongo, database::String, collection::String, pipeline::Array)::Array
    try
        mongo_collection = mongo.connection[database][collection]
        pipeline_bson = BSON(pipeline)
        result = aggregate(mongo_collection, pipeline_bson)
        if !isnothing(result)
            docs = Dict[]
            for item in result
                push!(docs, as_dict(item))
            end
            result = docs
        end
        return result
    catch e
        rethrow("Error while running aggregate: $e")
    end
end

function update_contents(mongo::Mongo, database::String, collection::String, query::Dict, updates::Dict)
    try
        mongo_collection = mongo.connection[database][collection]
        bson_query = BSON(query)
        bson_updates = BSON(updates)
        find_and_modify(mongo_collection, bson_query, update=bson_updates)
    catch e
        rethrow("Error while updating element in '$database.$collection': $e")
    end
end

#= USE

include("mongo_connection.jl") and use all module functions.

Create Mongo connection setting host and port:
host = "localhost"
port = 27017
mongo = Mongo(host, port)

query = Dict( "imdbId" => "tt0075314" )
projection = Dict( "_id" => 0, "averageRating" => 1, "imdbId" => 1 )
sort = Dict( "averageRating" => -1 )

find_one:
content = find_contents(mongo, db, col, query, projection, sort)

find_many:
contents = find_many_contents(mongo, db, col, query, projection, sort)

insert_one:
doc = Dict( "name" => "David", "age" => 15 )
insert_many_contents(mongo, db, col, docs)

insert_many:
docs = Dict[
    Dict( "name" => "David", "age" => 20 ),
    Dict( "name" => "Ricardo", "age" => 35 ),
    Dict( "name" => "Lucas", "age" => 54 )
]
insert_many_contents(mongo, db, col, docs)

aggregate:
pipeline = Dict[
    Dict( "\$match" => Dict( "startYear" => Dict( "\$gt" => "2015" )  ) ),
    Dict( "\$group" => Dict( "_id" => "\$startYear", "count" => Dict( "\$sum" => 1 ) ) ),
    Dict( "\$project" => Dict( "_id" => 0, "year" => "\$_id", "count" => 1 ) ),
    Dict( "\$sort" => Dict( "year" => -1 ) ),
]
result = run_aggregate(mongo, "testing", "imdbTitleBasics", pipeline)

update:
query = Dict( "imdbId" => "tt0103781" )
update = Dict( "\$set" => Dict( "genres" => "Short" ) )
update_contents( mongo, db, col, query, update )

=#