using Dates
import HTTP
import Formatting
import Base.Threads.@threads
include("mongo_connection_threads.jl")

using PyCall
pyimport_conda("bs4", "beautifulsoup4")
bs4 = pyimport("bs4").BeautifulSoup


base_url = "https://letterboxd.com/films/ajax/popular/this/week/size/small/page/{}/?esiAllowFilters=true"
content_type = "movie"


function process_scraping(content_type::String, url::String)
    page_step = 2
    n_pages = get_page_numbers(url)
    n_pages = 10
    println("n_pages :: $n_pages")

    for i in 1:page_step:n_pages
        println([x for x in i:(i+page_step)])
        crawlers(url, [x for x in i:(i+page_step)], content_type)
    end

end


function crawlers(url::String, pages_range, content_type::String)::Array
    
    chunks = Iterators.partition(pages_range, length(pages_range) ÷ Threads.nthreads())
    tasks = map(chunks) do chunk
        # Threads.@spawn proccess_crawlers(url, chunk, content_type)
        proccess_crawlers(url, chunk, content_type) # No using threads
        
    end
    
    fetch.(tasks) 
end


function proccess_crawlers(url::String, pages_range, content_type::String)
    for n_page in pages_range
        url =  Formatting.format(url, string(n_page))
        crawlers_result = get_crawlers(url)
        println("\nPAGE $n_page | $(length(crawlers_result)) items")
        if isnothing(crawlers_result) || length(crawlers_result)==0 continue end
        crawlers_item_result = Dict[]
        for item in crawlers_result
            item_result = get_scraping(item, content_type)
            if isnothing(item_result) continue end
            push!(crawlers_item_result, item_result)
        end
        insert(crawlers_item_result)
    end
end


function get_crawlers(url::String)
    host = "https://letterboxd.com"
    list_crawlers = Dict[]
    html = get_html(url)
    if isnothing(html)
        return
    end

    items = html.find_all("li", Dict( "class" => "listitem"))

    if isnothing(items)
        return []
    end 
    for item in items
        div_info = item.div
        if isnothing(div_info)
            continue
        end
        relative_link = div_info.get("data-target-link")
        content_id = div_info.get("data-film-id")

        if isnothing(relative_link) || isnothing(content_id)
            continue
        end

        link = host * relative_link
        rating = "";
        try
            rating = parse(Float64, item.get("data-average-rating"))
        catch
            rating = ""
        end 

        item_to_scrape = Dict(
            "url" => link,
            "id" => content_id,
            "rating" => rating
        )
        push!(list_crawlers, item_to_scrape)
    end

    return list_crawlers
end


function get_scraping(item, content_type::String)

    scraped_item = nothing;

    id = item["id"]
    url = item["url"]
    rating = item["rating"]

    html = get_html(url)

    if isnothing(html)
        return
    end 
    try
        section_header = html.find("section", Dict( "class" => "film-header-group"))

        title = nothing;
        try
            h1_title = section_header.find("h1")
            title = strip(h1_title.text)
        catch e
            print("\nERROR:\n$e\n")
        end

        imdb_id, tmdb_id = nothing, nothing;
        try
            a_tag_imdb = html.find("a", Dict("data-track-action" => "IMDb"))
            if !isnothing(a_tag_imdb) && !isnothing(a_tag_imdb.get("href"))
                imdb_link = a_tag_imdb.get("href")
                reg_match = match(r"tt\d+", imdb_link)
                if !isnothing(reg_match)
                    imdb_id = reg_match.match
                end
            end
            a_tag_tmdb = html.find("a", Dict("data-track-action" => "TMDb"))
            if !isnothing(a_tag_tmdb) && !isnothing(a_tag_tmdb.get("href"))
                tmdb_link = a_tag_tmdb.get("href")
                tmdb_id =  split( tmdb_link, "/" )[ end-1 ] # tmdb_link.split("/")[-2]
            end
        catch e
            print("\nERROR:\n$e\n")
        end

        comments = nothing;
        try
            comments = get_comments(url)
        catch e
            print("\nERROR COMMENTS:\n$e\n")
        end

        scraped_item = Dict(
            "platformCode" => "letterbox",
            "id" => id,
            "permalink" => url,
            "title" => title,
            "type" => content_type,
            
            "comments" => comments,

            "imdb_id" => imdb_id,
            "tmdb_id" => tmdb_id,
            
            "popularity" => Dict(
                # "votes" => votes,
                # "likes" => likes,
                # "views" => views,
                "rating" => rating,
            ),
            
            "createdAt" => string(today()),
        )

    catch e
        print("\nERROR WHILE SCRAPING ITEM:\n$e\n")
    end

    # print("\nSCRAPED ITEM:\n$scraped_item\n")

    return scraped_item
end


function get_page_numbers(url::String)::Int

    url = Formatting.format(url, string(1))
    println(url)
    html = get_html(url)
    if isnothing(html)
        return 0
    end

    movies_raw_number = html.find("p", class_="ui-block-heading").text

    total_movies = parse(Int,filter(x -> isdigit(x), movies_raw_number))
            
    movies = html.find_all("li", Dict("class" => "listitem"))

    total_pages = round(Int,total_movies / length(movies))

    return total_pages

end


function get_comments(url::String)::Array
    movie_reviews = []
    comment_page_limit = 1
    for i in 1:comment_page_limit
        all_reviews = []
        try
            html = get_html(url * "reviews/page/" * string(i))
            all_reviews = html.find_all("li", class_="film-detail")
            # print("\n$all_reviews\n")
        catch e
            print("\nERROR 1:\n$e\n")
            continue
        end
        for review in all_reviews
            try                
                review_text = "";
                buffer_review = review.find("div", class_="body-text -prose collapsible-text")
                if !isnothing(buffer_review.find("p", class_="contains-spoilers"))
                    review_text = buffer_review.find("div", "hidden-spoilers expanded-text").text
                else
                    review_text = buffer_review.text
                end
                push!(movie_reviews, review_text)
            catch e
                print("\nERROR 2:\n$e\n")
            end
        end
    end
    return movie_reviews
end


function metric_converter(metric::String)

    number = nothing;
    try
        metric = lowercase(metric)
        multipliers = Dict("k" => 1e3, "m" => 1e6, "b" => 1e9)

        suffix = str[end]
        number_str = str[1:end-1]

        number = parse(Float64, number_str)

        if haskey(multipliers, suffix)
            number *= multipliers[suffix]
        end
    catch e
        print("\nERROR METRIC CONVERTER:\n$e\n")
    end

    return number
end


function insert(contents::Array)
    try
        if length(contents) > 0
            # println("")
            # insert_many_contents(Mongo("localhost", 27017), "letterbox_data", "movies", contents)
            insert_many_contents("localhost", 27017, "letterbox_data", "movies", contents)
        end
    catch e
        print("\nERROR:\n$e\n")
    end
end


function get_html(url::String)

    tries = 0
    headers = Dict("User-Agent"=> "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36")

    while tries < 5
        try
            response = HTTP.request("GET", url, headers)
            body = String(response.body)
            html = bs4(body, features="html.parser")

            return html
        catch
            tries += 1
        end
    end
end


function main()
    process_scraping(content_type, base_url)
end


if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
