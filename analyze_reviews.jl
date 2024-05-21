include("mongo_connection.jl")
import VaderSentiment: SentimentIntensityAnalyzer


function get_reviews_items()::Array
    db = "testing"
    col_reviews = "collaborative_db"
    mongo = Mongo("localhost", 27017)

    query = Dict() # Dict( "imdb_id" => "tt1517268" )
    projection = Dict(  "_id" => 0, "imdb_id" => 1, "title" => 1, "type" => 1, "popularity" => 1, "comments" => 1 )
    items = find_many_contents(mongo, db, col_reviews, query, projection)

    return items
end


function analyze_comments(comments::Array)::Dict
    analyzer = SentimentIntensityAnalyzer
    positive_sum, negative_sum, neutral_sum, compound_sum = 0, 0, 0, 0
    for comment in comments
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
    return Dict( "positive" => round(positive_sum,digits=2), "neutral" => round(neutral_sum,digits=2), 
                 "negative" => round(negative_sum,digits=2), "compound" => round(compound_sum,digits=2) )
end


function upload_scores(item::Dict, scores::Dict)
    db = "testing"
    col_reviews = "collaborative_db"
    mongo = Mongo("localhost", 27017)

    query = Dict( "imdb_id" => item["imdb_id"] )
    upload = Dict( "\$set" => Dict( "reviews_analysis" => scores ) )
    
    update_contents(mongo, db, col_reviews, query, upload)
end


function reviews_analysis()
    println("PROCESSING REVIEWS.\n")
    
    items = get_reviews_items()
    if isnothing(items) | length(items) == 0
        throw("No items in '$db.$col_reviews'.")
    end
    
    i, n, m = 0, length(items), length( string( length(items) ) )
    println("There are $n items to process.")
    
    for item in items
        i += 1
        print(lpad(i, m), "/$n\r")
        scores = analyze_comments(item["comments"])
        if !isnothing(scores)
            upload_scores(item, scores)
        end
    end
    println("\n")
end


function main()
    reviews_analysis()
end


if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
