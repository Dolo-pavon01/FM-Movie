include("../BBDD/mongo_connection.jl")
using DataFrames
using CSV
using Dates
import Tables: rows


function log(x) println("$(now()) | $x") end

csv_filename = "tp_data_final.csv"

db_testing = "testing"

col_imdb_complete = "imdb_complete"
col_tmdb_complete = "tmdb_complete"
col_comments_rating = "collaborative_db"
col_titlebasics = "imdbTitleBasics"
col_titleratings = "imdbTitleRatings"
col_namebasics = "imdbNameBasics"
col_datatp = "data_tp_final"


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
    
mongo = Mongo("localhost", 27017)


function complete_imdb_nan(df)
    for col_name in names(df)
        try
            df[:, col_name] = replace.(df[:, col_name], "\\N" => missing)
            df[:, col_name] = replace.(df_items[:, col_name], "\\\\N" => missing)
        catch e
            nothing
        end
    end
    return df
end



function isnull(value)
    if ismissing(value) || value===nothing || value=="" || value=="nothing"
        return true
    end
    return false
end


function set_company_names(value)
    if value === nothing
        return nothing
    end
    companies = String[]
    for i in value
        push!(companies, String(i["Name"]))
    end
    return companies
end


function set_country(value)
    if value === nothing
        return nothing
    end
    return value[1]
end


function set_string_array(value)
    if value === nothing
        return nothing
    end
    values_list = String[]
    for i in value
        push!(values_list, String(i))
    end
    return values_list
end


function get_data_from_mongo(database::String, collection::String, query::Dict, projection::Dict)

    # log("Searching data in '$database.$collection'.")
    
    pipeline = [
        Dict( "\$match" => query ),
        Dict( "\$count" => "count" )
    ]
    
    result_count = run_aggregate(mongo, database, collection, pipeline)
    if length(result_count) == 0
        log("\tNo items found in '$database.$collection'.\n")
        return DataFrame
    end
    total_items = result_count[1]["count"]

    step = 1000000
    # log("\tThere are $total_items items to get.")

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
        # log("\t\t$n_got / $total_items")
    end
    
    if "imdbId" in names(df_items)
        dropmissing!(df_items, :imdbId);
    end

    df_items = complete_imdb_nan(df_items)

    # log("\t$(nrow(df_items)) total items got.\n")
    return df_items
end


# #### Imdb TitleBasics data
function get_title_basics()
    #=
        Returns DataFrame with columns: imdbId, type, year, duration, genres
    =#
    # query = Dict()
    query = Dict( "imdbId" => Dict( "\$in" => imdbs_list ) )
    projection = Dict(
        "_id" => 0,
        "imdbId" => "\$imdbId",
        "type" => "\$titleType",
        "year" => "\$startYear",
        "duration" => "\$runtimeMinutes",
        "genres" => "\$genres",
    )
    df_titlebasics = get_data_from_mongo(db_testing, col_titlebasics, query, projection)
    transform!(df_titlebasics, :genres .=> ByRow(x -> [ String(i) for i in split(x, ",") ]) .=> :genres)
    log("$(nrow(df_titlebasics)) items from $col_titlebasics ") # \n$(first(df_titlebasics, 2))\n")

    # log("\n\n$df_titlebasics\n\n")
    return df_titlebasics
end


# #### Imdb TitleRatings data
function get_title_ratings(df::DataFrame)
    #=
        Arguments
            df (DataFrame) with columns (at least) imdbId
        Returns with original dataframe with added columns rating, votes
    =#
    # query = Dict()
    query = Dict( "imdbId" => Dict( "\$in" => imdbs_list ) )
    projection = Dict(
        "_id" => 0,
        "imdbId" => "\$imdbId",
        "rating" => "\$averageRating",
        "votes" => "\$numVotes",
    )
    df_titleratings = get_data_from_mongo(db_testing, col_titleratings, query, projection)
    log("$(nrow(df_titleratings)) items from $col_titleratings ") # \n$(first(df_titleratings, 2))\n")
    df = leftjoin(df, df_titleratings, on=:imdbId) # Add columns rating, votes
    # log("\t -->> $(nrow(df)) ITEMS <<--\n")
    # log("\n\n$df\n\n")
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
    # query = Dict()
    query = Dict( "imdb_id" => Dict( "\$in" => imdbs_list ) )
    projection = Dict(
        "_id" => 0,
        "imdbId" => "\$imdb_id",
        "reviews" => "\$reviews_analysis.compound",
    )
    df_collaborativedb = get_data_from_mongo(db_testing, col_comments_rating, query, projection)
    df_collaborativedb = combine(groupby(df_collaborativedb, :imdbId), :reviews => maximum => :reviews) # EXPLICAR
    log("$(nrow(df_collaborativedb)) items from $col_comments_rating ") # \n$(first(df_collaborativedb, 2))\n")
    df = leftjoin(df, df_collaborativedb, on=:imdbId)
    # log("\t -->> $(nrow(df)) ITEMS <<--\n")
    # log("\n\n$df\n\n")
    return df
end


# #### Imdb NameBasics data (directors)
function get_name_basics(df::DataFrame)
    #=
        Arguments
            df (DataFrame) with columns (at least) imdbId
        Returns with original dataframe with added column directors
    =#
    query = Dict(
        "\$or" => [ Dict( "knownForTitles" => Dict( "\$regex" => value ) ) for value in imdbs_list ]
    )
    projection = Dict(
        "_id" => 0,
        "profession" => "\$primaryProfession",
        "name" => "\$primaryName",
        "titles" => "\$knownForTitles"
    )
    
    # df_namebasics = get_data_from_mongo(db_testing, col_namebasics, query, projection)
    # CSV.write("names_test.csv", df_namebasics)
    df_namebasics = DataFrame( CSV.read("names_test.csv", DataFrame) )
    
    df_namebasics = filter(row -> occursin( "director" , row.profession ) , df_namebasics)
    df_namebasics = df_namebasics[ : , [:name, :titles] ]
    transform!(df_namebasics, :titles .=> ByRow(x -> [ String(i) for i in split(x, ",") ]) .=> :titles)
    df_namebasics = DataFrame( [ ( name=row["name"], imdbId=val ) for row in eachrow(df_namebasics) for val in row[:titles] ] )
    df_namebasics = filter(row -> row.imdbId in imdbs_list , df_namebasics)
    df_directors = DataFrame()
    for i in groupby( df_namebasics, :imdbId )
        names_vector = [ String(_name) for _name in i[:, :name] ]
        imdb = i[1,"imdbId"]
        push!(df_directors, ( directors=names_vector, imdbId=imdb ))
    end
    df = leftjoin(df, df_directors, on=:imdbId)

    # log("\t -->> $(nrow(df)) ITEMS <<--\n")
    # log("\n\n$df\n\n")
    return df
end


# Imdb complete data 
function get_imdb_complete(df::DataFrame)
    #=
        Arguments
            df (DataFrame) with columns (at least) imdbId
        Returns with original dataframe with added columns cast, country, releaseDate, keywords, companies
    =#
    # query = Dict()
    query = Dict( "Id" => Dict( "\$in" => imdbs_list ) )
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

    df_imdb_complete = transform(df_imdb_complete, :companies .=> ByRow(x -> set_company_names(x)) .=> :companies)
    df_imdb_complete = transform(df_imdb_complete, :country .=> ByRow(x -> set_country(x)) .=> :country)

    for col_name in [ "cast", "keywords" ]
        df_imdb_complete = transform(df_imdb_complete, col_name .=> ByRow(x -> set_string_array(x)) .=> col_name)
    end

    log("$(nrow(df_imdb_complete)) items from $col_imdb_complete ") # \n$(first(df_imdb_complete, 2))\n")

    df = leftjoin(df, df_imdb_complete, on=:imdbId) # Add columns: cast, country, releaseDate, keywords, companies

    # log("\t -->> $(nrow(df)) ITEMS <<--\n")
    # log("\n\n$df\n\n")
    return df
end


# Tmdb complete data
function get_tmdb_complete()
    # query = Dict( "imdbId" => Dict( "\$ne" => nothing ) )
    query = Dict( "imdbId" => Dict( "\$in" => imdbs_list ) )
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

    df_tmdb = transform(df_tmdb, :companies .=> ByRow(x -> set_company_names(x)) .=> :companies)
    df_tmdb = transform(df_tmdb, :country .=> ByRow(x -> set_country(x)) .=> :country)
    for col_name in [ "genres", "companies", "cast", "directors", "keywords" ]
        df_tmdb = transform(df_tmdb, col_name .=> ByRow(x -> set_string_array(x)) .=> col_name)
    end

    log("$(nrow(df_tmdb)) items from $col_tmdb_complete ") # \n$(first(df_tmdb, 2))\n")

    return df_tmdb
end


function complete_data(df, df_tmdb)

    cols = names(df)

    log("Completing data for $(cols) rows:\n")

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
        # log("$(names(df))")

        # log("\t\tGetting null data from df")
        null_data = df[ [  isnull(i) for i in df[:, field] ] , : ]
        if isempty(null_data)
            log("\t\t\tNo null data")
            continue
        end

        # log("\t\tGetting not null data from df")
        not_null_data = df[ [  !(isnull(i)) for i in df[:,field] ] , : ]

        df_tmdb_info = df_tmdb[ : , ["imdbId", field] ]
        # log("\t\tGetting not null data from tmdb df")
        df_tmdb_info = df_tmdb_info[ [  !(isnull(i)) for i in df_tmdb_info[:,field] ] , : ]

        # log("\t\tRemoving field $field from df")
        select!(null_data, Not([field]))
        
        # log("\t\tJoin data")
        null_data = leftjoin(null_data, df_tmdb_info, on=:imdbId)

        # log("\t\tConcat data")
        df = vcat( null_data[:, cols] , not_null_data[:, cols] )

        # println("\n\n")
    end
	
    return df
end


function insert_to_mongo(df)

    log("Inserting data: $(nrow(df)) rows:\n")
    items = rows(df) |> collect

    items_dict = []
    for item in items
        a = Dict(names(item) .=> values(item))
        # print(a)
        push!(items_dict, a)
    end

    log("\tInserting to mongo...")

    # insert_many_contents(mongo, db_testing, col_datatp, items_dict)
    for item in items_dict
        try
            insert_content(mongo, db_testing, col_datatp, item)
        catch e
            println("\n\nERROR: $e \n$item\n\n")
            break
        end
    end
end


function set_nothing_to_missing(df::DataFrame)

    println("\n\n")
    log("Setting nothing values to missing.")
    columns = names(df)[1:7]
    # columns = [ "duration" ]

    # df = df[:, [ "duration", "year", "type", "imdbId", "reviews", "rating" ] ]
    println("\n\n$( df[ : , columns ] )\n\n")

    index, n = 1, length(columns)
    for column in columns
        log("\tColumn '$column' $index/$n")
        index += 1
        df_nulls = nothing
        try
            df_nulls = df[ [ isnull(i) for i in df[:,column] ] , : ]
        catch
            print("\n\nERROR GETTING NULL VALUES FOR COLUMN:\n$(df[:,column])\n\n")
            continue
        end
        # println("\nNULL VALUES\n$df_nulls\n")
        if isempty(df_nulls)
            log("\t\tNo nulls values\n\n")
            continue
        end
        println("\t\tNULL VALUES\n$df_nulls\n\n")

        # df_not_nulls = df[ [  i!="nothing" for i in df[:,column] ] , : ]
        # println("\nNOT NULL VALUES\n$df_not_nulls\n")

        # df_nulls[ ! , column ] .= missing
        # println("\nNEW NULL VALUES\n$df_nulls\n")

        # df = vcat( df_not_nulls , df_nulls )
    end

    # println("\n\n$df\n\n")

	return df
end


function main()

    # df = DataFrame()
|
    df = get_title_basics()
    df = get_title_ratings(df)
    df = get_reviews(df)
    df = get_name_basics(df)
    df = get_imdb_complete(df)
    df_tmdb = get_tmdb_complete()
    
    # CSV.write("out.csv", df, transform=(col, val) -> something(val, missing))
    # CSV.write("out2.csv", df_tmdb, transform=(col, val) -> something(val, missing))
    
    df = complete_data(df, df_tmdb)

    # df = df.fillna(value=nan).replace(to_replace=nan, value=None)
    # df = df.replace(to_replace="", value=nothing)
    # df = df.replace(to_replace="\\N", value=nothing)
    # df = df.replace(to_replace="\\\\N", value=nothing)

    # CSV.write("out3.csv", df, transform=(col, val) -> something(val, missing))
    
    df = set_nothing_to_missing(df)

    # insert_to_mongo(df)

    # df = df[ [  !(isnull(i)) for i in df[:, :rating] ] , : ]
    # CSV.write(csv_filename, df)

    # println("\n\n$df\n\n")

end


if abspath(PROGRAM_FILE) == @__FILE__
    main()
end


#=

VIDEO:

5 minutos sobre historia, razon de ser, uso.
Muestra y explicación de código: 
    Demostrar que sabemos programar en el lenguaje.
    No mostrar cosas triviales (no es necesario código super complejo).
Hablar coloquialmente para hacerlo dinamico y entretenido.


Hacer subsets para las ejecuciones a mostrar en el video y para que lo pueda ejecutar el profe.
Realizar un proceso paralelo para que guarde y lea CSVs (truncados para hacerlos pequeños).
Para hacer eso filtrar por una lista de imdb_ids.

Comparacion OZ con Julia
    Pattern matching

=#
