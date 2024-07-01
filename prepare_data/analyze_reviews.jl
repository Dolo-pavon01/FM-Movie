module module_analyze_reviews
include("../BBDD/mongo_connection.jl")
import VaderSentiment: SentimentIntensityAnalyzer
using DataFrames
using CSV

function get_reviews_items(mongo, limit::Int)::Array
    db = "testing"
    col_reviews = "collaborative_db"

    query = Dict("reviews_analysis" => nothing, "imdb_id" => Dict("\$ne" => nothing))
    projection = Dict("_id" => 0, "imdb_id" => 1, "comments" => 1)
    items = find_many_contents(mongo, db, col_reviews, query=query, projection=projection, sort=Dict(), limit=limit)

    return items
end


function get_n_reviews_items(mongo)::Int
    db = "testing"
    col_reviews = "collaborative_db"

    query = Dict("reviews_analysis" => nothing, "imdb_id" => Dict("\$ne" => nothing))
    projection = Dict("_id" => 0, "imdb_id" => 1, "comments" => 1)
    items = find_many_contents(mongo, db, col_reviews, query=query, projection=projection)

    n = length(items)

    items = nothing

    return n
end


function analyze_comments(comments::Array)::Float64

    analyzer = SentimentIntensityAnalyzer
    positive_sum, negative_sum, neutral_sum, compound_sum = 0, 0, 0, 0
    for comment in comments
        if length(comment) > 550
            for i = 500:-1:0
                try
                    comment = SubString(comment, 1, i)
                    break
                catch e
                    nothing
                end
            end
        end
        scores = Dict()
        try
            scores = analyzer(comment).polarity_scores
        catch e
            println("\nError: $e")
            continue
        end
        positive_sum += scores["pos"]
        negative_sum += scores["neg"]
        neutral_sum += scores["neu"]
        compound_sum += scores["compound"]
    end

    # return Dict("positive" => round(positive_sum, digits=2), "neutral" => round(neutral_sum, digits=2),
    #     "negative" => round(negative_sum, digits=2), "compound" => round(compound_sum, digits=2))

    return  round(compound_sum, digits=2)
end


function upload_scores(mongo::Mongo, item::Dict, scores::Dict)
    db = "testing"
    col_reviews = "collaborative_db"

    query = Dict("imdb_id" => item["imdb_id"])
    upload = Dict("\$set" => Dict("reviews_analysis" => scores))

    update_contents(mongo, db, col_reviews, query, upload)
end


function string_a_lista(s::Union{Missing, String})
    if ismissing(s)
        return missing
    end

    s = replace(s, "[" => "")
    s = replace(s, "]" => "")
    s = strip(s)

    list = split(s, ",")
    list = [strip(palabra, [' ', '\'']) for palabra in list]
    return [string.(palabra) for palabra in list]
end

function reviews_analysis_csv()
    # println("PROCESSING REVIEWS.\n")

    file_path = "./data_output/letterbox.csv"
    df = CSV.read(file_path, DataFrame)

    n = 100000
    i, m = 0, 6
    step = 300

    println(lpad(i, m), "/$n")
    items = [Dict(col => df[row, col] for col in names(df)) for row in 1:nrow(df)]

    j = 0
    for item in items
        j += 1
        scores = analyze_comments(string_a_lista(item["comments"]))
        if !isnothing(scores)
            item["scores"] = scores
        end
    end

    df  = DataFrame(items)
    CSV.write("./data_output/reviews.csv", df)
    println("\n")
end


function reviews_analysis_with_mongo()
    # println("PROCESSING REVIEWS.\n")

    mongo = Mongo("localhost", 27017)

    n = get_n_reviews_items(mongo)


    i, m = 0,6
    step = 300
    println("There are $n items to process.")

    while i < n

        i += step
        println(lpad(i, m), "/$n")
        items = get_reviews_items(mongo, step)

        j = 0
        for item in items
            j += 1
            println("$j / $step")
            imdb_id = item["imdb_id"]
            scores = analyze_comments(item["comments"])
            if !isnothing(scores)
                upload_scores(mongo, item, scores)
                # break
            end

            delete!(item, "comments")
        end
    end

    println("\n")
end

function process_ANALYZE()
    reviews_analysis_csv()
end


if abspath(PROGRAM_FILE) == @__FILE__
    process_ANALYZE()
end

export process_ANALYZE
end #module_analyze_reviews