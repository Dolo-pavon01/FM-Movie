using HTTP
using CodecZlib
using Dates
include("../BBDD/mongo_connection.jl")

hora_start_script = now()
println("Fecha y hora actual: ", hora_start_script)


function getDataFromURLFile(imdb_file::String)

    try
        response = HTTP.request("GET", imdb_file)
        body = response.body
        filehandler = GzipDecompressorStream(IOBuffer(body))

        return filehandler
    catch e
        rethrow("Error getting data from URL: $e")
    end

end

function proccess_data(data::Array, header::Array)

    subdata_imdb = Dict()
    for (key, value) in zip(header, data)
        try
            value = parse(Float64, value)
        catch
        end
        subdata_imdb[key] = value
    end

    return subdata_imdb

end

function process_imdb_payload(imdb_file::String, callback::Function)

    try
        imdb_data = getDataFromURLFile(imdb_file)
        filename_collection = split(imdb_file, "/")
        filename_collection = replace(filename_collection[length(filename_collection)], ".tsv.gz" => "")
        filename_collection = "TEST____" * replace(filename_collection, "." => "_")

        index_header = 1
        header = []

        counter = 0
        limit = 10000
        len = 0
        imdb_payload = Dict[]

        println("Normalize  ", filename_collection)

        for linea in eachline(imdb_data)
            linea = split(linea, "\t")
            if index_header == 1
                header = linea
                index_header = 0
            elseif counter >= limit
                len = len + counter
                print("Insertando en $(filename_collection)   ::::::  Cantidad de Elementos Insertados al momento: $(len)\r")
                callback(filename_collection, imdb_payload)
                imdb_payload = Dict[]
                counter = 0
            end
            push!(imdb_payload, proccess_data(linea, header))

            counter = counter + 1
        end

        if counter >= 0
            len += counter
            println("Insertando en $(filename_collection)   ::::::  Cantidad de Elementos Insertados al momento: $(len)")
            callback(filename_collection, imdb_payload)
        end
    catch
        rethrow("Error proccesing data: $e")
    end

end

function proccess_imdb_files(imdb_files::Vector{String}, callback::Function)

    hour_func_start = now()

    for imdb_file in imdb_files
        println("Extract From ::::::  ", imdb_file)
        process_imdb_payload(imdb_file, callback)
        println("Tardanza del archivo $imdb_file ::  ", ((now() - hour_func_start) / Millisecond(1)) / 60000)
        hour_func_start = now()

        println()
        println()
    end
    println("Tardanza total ::  ", ((now() - hora_start_script) / Millisecond(1)) / 60000)
end


function upload_database(host::String, port::Int, database::String)
    return function (collection::String, contents::Array)
        insert_many_contents(host, port, database, collection, contents)
    end
end

try

    imdb_files = [
        "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "https://datasets.imdbws.com/title.episode.tsv.gz",
        "https://datasets.imdbws.com/title.basics.tsv.gz",
        "https://datasets.imdbws.com/title.crew.tsv.gz",
        "https://datasets.imdbws.com/name.basics.tsv.gz",
        "https://datasets.imdbws.com/title.akas.tsv.gz",
        "https://datasets.imdbws.com/title.principals.tsv.gz"
    ]

    host = "localhost"
    port = 27017
    database = "TEST___imdb_data"
    proccess_imdb_files(imdb_files, upload_database(host, port, database))

catch error
    println(error)
end