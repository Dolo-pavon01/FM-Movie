include("./extract_data/imdb_data.jl")
include("./extract_data/letterbox.jl")
include("./prepare_data/analyze_reviews.jl")
include("./prepare_data/unify_data.jl")

using .module_imdb_data
using .module_letterbox
using .module_analyze_reviews
using .module_unify_data
using .module_letterbox


function print_title()
    println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    println("FM-MOVIE  :::: MOVIES SCORES :: ANALYSIS AND PREDICTIONS")
    println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::")

end

function main()

    print_title()

    println("::::::::::::STARTING DATA EXTRACTION ::::::::::::::::::::")
    println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    println()
    println()


    println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    println(":::::::::::::::: EXTRACT FROM IMDB ::::::::::::::::::::::")
    println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    process_IMDB()

    println()
    println()
    println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    println(":::::::::::::::: EXTRACT FROM LETTERBOX :::::::::::::::::")
    println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    process_Letterbox()


    process_ANALYZE()
    process_UNIFY()
end


if abspath(PROGRAM_FILE) == @__FILE__
    main()
end