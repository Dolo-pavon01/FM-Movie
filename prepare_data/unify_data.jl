module module_unify_data

include("../BBDD/mongo_connection.jl")
using DataFrames: DataFrame, replace, ismissing, names, values, nrow, vcat, dropmissing, transform!, 
                    ByRow, leftjoin, combine, groupby, filter, occursin, unique, isempty, select!, Not
using CSV
using Dates
import Tables: rows


csv_filename = "tp_data_final.csv"

db_testing = "testing"

col_imdb_complete = "imdb_complete"
col_tmdb_complete = "tmdb_complete"
col_comments_rating = "collaborative_db"
col_titlebasics = "imdbTitleBasics"
col_titleratings = "imdbTitleRatings"
col_namebasics = "imdbNameBasics"
col_datatp = "data_tp_final"


imdbs_list_test = [
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
    "\\N"
]
    
mongo = Mongo("localhost", 27017)


function log(x) println("$(now()) | $x") end


function set_nulls_to_missing(df::DataFrame)
    log("Setting null values to missing.")
    for column in names(df)
        df_nulls = df[ [ isnull(i) for i in df[:,column] ] , : ]
        df_nulls[ ! , column ] .= missing
        df_not_nulls = df[ [  !(isnull(i)) for i in df[:,column] ] , : ]
        df = vcat( df_not_nulls , df_nulls )
    end
    return df
end


function isnull(value::Any)
    if ismissing(value) || value===nothing || value=="" || value=="nothing" || value=="\\N" || value=="\\\\N"
        return true
    end
    return false
end


function set_company_names(value::Any)
    if isnull(value)
        return missing
    end
    companies = String[]
    for i in value
        push!(companies, String(i["Name"]))
    end
    return companies
end


function set_country(value::Any)
    if isnull(value)
        return missing
    end
    return value[1]
end


function reset_string_array(value::Any)
    if isnull(value)
        return missing
    end
    values_list = String[]
    for i in value
        push!(values_list, String(i))
    end
    return values_list
end


function set_array_from_string(value::Any)
    if isnull(value)
        return missing
    end
    string_array = [ String(i) for i in split(value, ",") ]
    return string_array
end


function get_data_from_mongo(database::String, collection::String, query::Dict, projection::Dict)

    log("Searching data in '$database.$collection'.")
    
    pipeline = [
        Dict( "\$match" => query ),
        Dict( "\$count" => "count" )
    ]
    
    result_count = run_aggregate(mongo, database, collection, pipeline)
    if length(result_count) == 0
        log("\tNo items found in '$database.$collection'.\n")
        return DataFrame()
    end
    total_items = result_count[1]["count"]
    log("\tThere are $total_items items to get.")

    step = 1000000
    n_got = 0
    df_items = DataFrame()

    for start in 0 : step : total_items+1
        pipeline = [
            Dict( "\$match" => query ),
            Dict( "\$skip" => start ),
            Dict( "\$limit" => step ),
            Dict( "\$project" => projection ),
        ]
        
        items = run_aggregate(mongo, database, collection, pipeline)
        
        df_current = DataFrame(items)
        # df_current = vcat(DataFrame.(items)...)
        
        df_items = vcat( df_items, df_current )
        
        n_got += length(items)
        log("\t\t$n_got / $total_items")
    end
    
    df_items = set_nulls_to_missing(df_items)
    log("\t$(nrow(df_items)) total items got")
    
    if "imdbId" in names(df_items)
        df_items = dropmissing(df_items, :imdbId);
        log("\t\tUnique by imdbId $(nrow(unique(df_items, :imdbId))).")
    end
    println("")
        
    return df_items
end


# #### Imdb TitleBasics data
function get_title_basics()
    #=
        Returns DataFrame with columns: imdbId, type, year, duration, genres
    =#
    # query = Dict( "imdbId" => Dict( "\$in" => imdbs_list_test ) )
    query = Dict()
    projection = Dict(
        "_id" => 0,
        "imdbId" => "\$imdbId",
        "type" => "\$titleType",
        "year" => "\$startYear",
        "duration" => "\$runtimeMinutes",
        "genres" => "\$genres",
    )
    df_titlebasics = get_data_from_mongo(db_testing, col_titlebasics, query, projection)
    transform!(df_titlebasics, :genres .=> ByRow(x -> set_array_from_string(x)) .=> :genres)

    return df_titlebasics
end


# #### Imdb TitleRatings data
function get_title_ratings(df::DataFrame)
    #=
        Arguments
            df (DataFrame) with columns (at least) imdbId
        Returns with original dataframe with added columns rating, votes
    =#
    # query = Dict( "imdbId" => Dict( "\$in" => imdbs_list_test ) )
    query = Dict()
    projection = Dict(
        "_id" => 0,
        "imdbId" => "\$imdbId",
        "rating" => "\$averageRating",
        "votes" => "\$numVotes",
    )
    df_titleratings = get_data_from_mongo(db_testing, col_titleratings, query, projection)
    df = leftjoin(df, df_titleratings, on=:imdbId) # Add columns rating, votes
    
    return df
end


# #### Letterboxd reviews data 
# Buscar ultima fecha (createdAt) y filtrar asi
function get_reviews(df::DataFrame)
    #=
        Arguments
            df (DataFrame) with columns (at least) imdbId
        Returns with original dataframe with added column reviews
    =#
    # query = Dict( "reviews_analysis" => Dict( "\$ne" => nothing ), "imdb_id" => Dict( "\$in" => imdbs_list_test ) )
    query = Dict( "reviews_analysis" => Dict( "\$ne" => nothing ) )
    projection = Dict(
        "_id" => 0,
        "imdbId" => "\$imdb_id",
        "reviews" => "\$reviews_analysis.compound",
    )
    df_collaborativedb = get_data_from_mongo(db_testing, col_comments_rating, query, projection)
    # println("\n\nUNIQUE BY imdbId: $(nrow(unique(df_collaborativedb, :imdbId)))\n\n")
    df_collaborativedb = combine(groupby(df_collaborativedb, :imdbId), :reviews => maximum => :reviews)
    # println("\n\nN ROWS AFTER GROUPING: $(nrow(df_collaborativedb))\n\n")
    df = leftjoin(df, df_collaborativedb, on=:imdbId)
    
    return df
end


# #### Imdb NameBasics data (directors)
function get_name_basics(df::DataFrame)
    #=
        Arguments
            df (DataFrame) with columns (at least) imdbId
        Returns with original dataframe with added column directors
    =#
    # query = Dict( "\$or" => [ Dict( "knownForTitles" => Dict( "\$regex" => value ) ) for value in imdbs_list_test ] )
    query = Dict()
    projection = Dict(
        "_id" => 0,
        "profession" => "\$primaryProfession",
        "name" => "\$primaryName",
        "titles" => "\$knownForTitles"
    )
    
    df_namebasics = get_data_from_mongo(db_testing, col_namebasics, query, projection)
    # CSV.write("names_test.csv", df_namebasics)
    # df_namebasics = DataFrame( CSV.read("names_test.csv", DataFrame) )
    
    df_namebasics = filter(row -> occursin( "director" , row.profession ) , df_namebasics)
    df_namebasics = df_namebasics[ : , [:name, :titles] ]
    transform!(df_namebasics, :titles .=> ByRow(x -> [ String(i) for i in split(x, ",") ]) .=> :titles)
    df_namebasics = DataFrame( [ ( name=row["name"], imdbId=val ) for row in eachrow(df_namebasics) for val in row[:titles] ] )
    # df_namebasics = filter(row -> row.imdbId in imdbs_list_test , df_namebasics)
    df_directors = DataFrame()
    for i in groupby( df_namebasics, :imdbId )
        names_vector = [ String(_name) for _name in i[:, :name] ]
        imdb = i[1,"imdbId"]
        push!(df_directors, ( directors=names_vector, imdbId=imdb ))
    end
    df = leftjoin(df, df_directors, on=:imdbId)

    return df
end


# Imdb complete data 
function get_imdb_complete(df::DataFrame)
    #=
        Arguments
            df (DataFrame) with columns (at least) imdbId
        Returns with original dataframe with added columns cast, country, releaseDate, keywords, companies
    =#
    # query = Dict( "Id" => Dict( "\$in" => imdbs_list_test ) )
    query = Dict()
    projection = Dict(
        "_id" => 0,
        "imdbId" => "\$Id",
        "cast" => "\$Cast",
        "country" => "\$Country",
        "releaseDate" => "\$ReleaseDate",
        "keywords" => "\$Keywords",
        "companies" => "\$Companies",
    )
    df_imdb_complete = get_data_from_mongo(db_testing, col_imdb_complete, query, projection)

    transform!(df_imdb_complete, :companies .=> ByRow(x -> set_company_names(x)) .=> :companies)
    transform!(df_imdb_complete, :country .=> ByRow(x -> set_country(x)) .=> :country)
    for col_name in [ "cast", "keywords" ]
        transform!(df_imdb_complete, col_name .=> ByRow(x -> reset_string_array(x)) .=> col_name)
    end
    df = leftjoin(df, df_imdb_complete, on=:imdbId) # Add columns: cast, country, releaseDate, keywords, companies

    return df
end


# Tmdb complete data
function get_tmdb_complete()
    # query = Dict( "imdbId" => Dict( "\$in" => imdbs_list_test ) )
    query = Dict( "imdbId" => Dict( "\$ne" => nothing ) )
    projection = Dict(
        "_id" => 0,
        "imdbId" => 1,
        "releaseDate" => "\$ReleaseDate",
        "votes" => "\$vote_count",
        "rating" => "\$vote_average",
        "duration" => "\$Duration",
        "genres" => "\$Genres",
        "country" => "\$Country",
        "keywords" => "\$Keywords",
        "cast" => "\$Cast",
        "companies" => "\$Companies",
        "directors" => "\$Directors"
    )
    df_tmdb = get_data_from_mongo(db_testing, col_tmdb_complete, query, projection)
    df_tmdb = unique(df_tmdb, :imdbId)

    transform!(df_tmdb, :companies .=> ByRow(x -> set_company_names(x)) .=> :companies)
    transform!(df_tmdb, :country .=> ByRow(x -> set_country(x)) .=> :country)
    for col_name in [ "genres", "companies", "cast", "directors", "keywords" ]
        transform!(df_tmdb, col_name .=> ByRow(x -> reset_string_array(x)) .=> col_name)
    end

    return df_tmdb
end


function complete_data(df::DataFrame, df_tmdb::DataFrame)

    cols = names(df)

    log("Completing data for $(nrow(df)) rows:")

    fields = [
		"releaseDate",
		"votes",
		"rating",
		"duration",
        "genres",
        "country",
        "keywords",
        "cast",
        "companies",
        "directors",
    ]

    index = 0
    for field in fields
        index += 1
        log("\tField '$field' $(index)/$(length(fields))")

        null_data = df[ [  isnull(i) for i in df[:, field] ] , : ]
        # null_data = df[ ismissing.( df[:,field] ) , : ]
        if isempty(null_data)
            log("\t\t\tNo null data")
            continue
        end

        select!(null_data, Not([field])) # Remove columns 'field'
        df_tmdb_info = df_tmdb[ [  !(isnull(i)) for i in df_tmdb[:,field] ] , ["imdbId", field] ]
        # df_tmdb_info = df_tmdb[ [ !(ismissing.(i)) for i in df_tmdb[:,field] ] , ["imdbId", field] ]
        null_data = leftjoin(null_data, df_tmdb_info, on=:imdbId)
        
        not_null_data = df[ [  !(isnull(i)) for i in df[:,field] ] , : ]
        # not_null_data = df[ [ !(ismissing.(i)) for i in df[:,field] ] , : ]

        df = vcat( null_data[:, cols] , not_null_data[:, cols] )
    end
    println("")
	
    return df
end


function insert_to_mongo(df::DataFrame)

    log("Inserting data: $(nrow(df)) rows:")

    for column in names(df)
        df_nulls = df[ [ ismissing(i) for i in df[:,column] ] , : ]
        df_nulls[ ! , column ] .= nothing
        df_not_nulls = df[ [  !(isnull(i)) for i in df[:,column] ] , : ]
        df = vcat( df_not_nulls , df_nulls )
    end

    items_dict = [ Dict(col => df[row, col] for col in names(df)) for row in 1:nrow(df) ]

    log("\tInserting to mongo...")
    insert_many_contents(mongo, db_testing, col_datatp, items_dict)
end


function process_UNIFY()
    log("STARTING PROCESS TO UNIFY DATA.\n\n")

    df = get_title_basics()
    df = get_title_ratings(df)
    df = get_reviews(df)
    df = get_name_basics(df)
    df = get_imdb_complete(df)
    df_tmdb = get_tmdb_complete()
    
    df = complete_data(df, df_tmdb)
    df = set_nulls_to_missing(df)
    df_ratings_notnull = dropmissing(df, :rating);
    
    insert_to_mongo(df)
    CSV.write(csv_filename, df_ratings_notnull)

    # println("\n\n$df\n\n")
end


if abspath(PROGRAM_FILE) == @__FILE__
    process_UNIFY()
end

export process_UNIFY
end # module_unify_data