include("mongo_connection.jl")
using DataFrames
using CSV
using Dates


csv_filename = "tp_data.csv"

db_testing = "testing"

col_imdb_complete = "imdb_complete"
col_tmdb_complete = "tmdb_complete"
col_comments_rating = "collaborative_db"
col_titlebasics = "imdbTitleBasics"
col_titleratings = "imdbTitleRatings"
col_namebasics = "imdbNameBasics"
col_datatp = "data_tp"


mongo = Mongo("localhost", 27017)

function log(x) println("$(now()) | $x") end


imdbs_list = [
    "tt10133702",
    "tt10333266",
    "tt10350420",
    "tt10399902",
    "tt10402396",
    "tt10469410",
    "tt10712472",
    "tt11244166",
    "tt11257606",
    "tt11281192",
]



function get_data_from_mongo(database::str, collection::str, query::Dict, projection::Dict)

    log(f"Searching data in '{database}.{collection}'.")
    df = DataFrame()
    
    pipeline = [
        Dict( "\$match" => query ),
        Dict( "\$count" => "count" )
    ]

    result_count = run_aggregate(mongo, database, collection, pipeline)
    total_items = result_count["count"]

    step = 1000000
    log("\tThere are $total_items to get.")

    for start in range(0, total_items+1, step)
        pipeline = [
            Dict( "\$skip" => start ),
            Dict( "\$limit" => step ),
            Dict( "\$match" => query ),
            Dict( "\$project" => projection ),
        ]

        items = run_aggregate(mongo, database, collection, pipeline)
        vcat(DataFrame.(items)...)
        
        log("\t\t$(df.shape[0]) / $total_items".ljust(100))
    end

    if "imdbId" in names(df)
        df = df.dropna(subset="imdbId")
        dropmissing!(df, :imdbId);
    end

    for col_name in names(df)
        df[:, col_name] = replace.(df[:, col_name], "\\N" => nothing)
        df[:, col_name] = replace.(df[:, col_name], "\\\\N" => nothing)
    end

    log("\t$(df.shape[0]) total items got.\n")
    return df

end


query_imdbs = {} # { 'imdbId': { '$in': imdbs_list } }



