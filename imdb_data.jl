using HTTP
using CodecZlib
using Dates


include("mongo_connection.jl")

fecha_hora_actual = now()
println("Fecha y hora actual: ", fecha_hora_actual)


function getDataFromURLFile(imdb_file::String)

    i = 0
    while i < 5
        try
            # println("GETTING  ::::::  ", imdb_file)
            response = HTTP.request("GET", imdb_file)
            body = response.body
            filehandler = GzipDecompressorStream(IOBuffer(body))

            return filehandler
        catch e
            println(e)
            i += 1
        end
    end

end


function proccess_data(data, header)

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



function get_imdb_payload(imdb_file::String, callback::Function)

    imdb_data = getDataFromURLFile(imdb_file)
    filename_collection = split(imdb_file, "/")
    filename_collection = replace(filename_collection[length(filename_collection)], ".tsv.gz" => "")
    filename_collection = "TEST___2__" * replace(filename_collection, "." => "_")

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
            println("Insertando en $(filename_collection)   ::::::  Cantidad de Elementos Insertados al momento: $(len)")
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
end


function proccess_imdb_files(imdb_files::Vector{String}, callback::Function)

    for imdb_file in imdb_files
        println("Extract From ::::::  ", imdb_file)
        get_imdb_payload(imdb_file, callback)
    end
    println("Tardanza ::  ", ((now() - fecha_hora_actual) / Millisecond(1)) / 60000)
end

imdb_files = [
    "https://datasets.imdbws.com/title.ratings.tsv.gz",
    # "https://datasets.imdbws.com/title.episode.tsv.gz",
    # "https://datasets.imdbws.com/title.basics.tsv.gz",
    # "https://datasets.imdbws.com/title.crew.tsv.gz",
    # "https://datasets.imdbws.com/name.basics.tsv.gz",
    # "https://datasets.imdbws.com/title.akas.tsv.gz",
    # "https://datasets.imdbws.com/title.principals.tsv.gz"
]


# Mongo connection

function upload_database(localMongo::Mongo, database::String)
    return function (collection::String, contents::Array)
        insert_many_contents(localMongo, database, collection, contents)
    end
end

try
    localMongo = Mongo("localhost", 27017)

    println(localMongo)
    database = "TEST___imdb_data"
    proccess_imdb_files(imdb_files, upload_database(localMongo, database))

catch error
    println(error)
end