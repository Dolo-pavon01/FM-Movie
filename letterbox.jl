import HTTP
import Gumbo
import Formatting
import Base.Threads.@threads
include("extract_data_with_threads/mongo_connection_threads.jl")

using PyCall
bs4 = pyimport("bs4").BeautifulSoup


base_url = "https://letterboxd.com/films/ajax/popular/this/week/size/small/page/{}/?esiAllowFilters=true"
content_type = "movie"

function process_scraping(content_type::String, url::String)
    page_step = 100
    n_pages = get_page_numbers(url)
    println("n_pages :: $n_pages")

    for i in 1:page_step:n_pages
        crawlers(content_url, [x for x in i:(i+page_step)], content_type)
    end

end


function proccess_crawlers(url::String, pages_range::Array, content_type::String)
    for n_page in pages_range
        url =  Formatting.formatting(url, string(n_page))
        crawlers_result = get_crawlers(url)
        crawlers_item_result = Dict[]
        for item in crawlers_result
            push!(crawlers_item_result, get_scraping(item, content_type))
        end
        insert(crawlers_item_result)
    end
end


function crawlers(url::String, pages_range::Array, content_type::String)::Array

    chunks = Iterators.partition(pages_range, length(pages_range) ÷ Threads.nthreads())
    tasks = map(chunks) do chunk
        Threads.@spawn proccess_crawlers(url, chunk, content_type)
    end

    fetch.(tasks) 
end


function get_crawlers(url::String)
    host = "https://letterboxd.com"
    list_crawlers = Dict[]
    soup = get_html(url)
    if isnothin(soup)
        return
    end

    items = soup.find_all("li", {"class": "listitem"})

    if isnothing(items)
        return []

    for item in items
        div_info = item.div
        if isnothing(div_info)
            continue
        relative_link = div_info.get("data-target-link")
        content_id = div_info.get("data-film-id")

        if !isnothing(relative_link) && 
            !isnothing(content_id)
            continue

        link = host + relative_link
        try
            rating = parse(Float64, item.get("data-average-rating"))
        catch
            rating = nothing
        end 

        item_to_scrape = {
            "url": link,
            "id": content_id,
            "rating": rating
        }
        push!(list_crawlers, item_to_scrape)
    end

    return list_crawlers
end


function get_scraping(item, content_type::String)

    # id = item["id"]
    # url = item["url"]
    # rating = item["rating"]

    # html = get_html(url)

    # if isnothing(html)
    #     return
    
    

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
    comment_page_limit = 5
    for  i in 1:comment_page_limit
        html = get_html(url*"reviews/page/"*String(i))
        all_reviews = soup.find_all("li", class_="film-detail")
        for review in all_reviews
            review_text;
            buffer_review = review.find("div", class_="body-text -prose collapsible-text")
            if buffer_review.find("p", class_="contains-spoilers")
                review_text = review2.find("div", "hidden-spoilers expanded-text").text
            else
                review_text = buffer_review.text
            end
            push!(movie_reviews, review_text)
        end
    end
    return movie_reviews
end


function metric_converter(metric::String)

    number;
    try
        metric = lowercase(metric)
        multipliers = Dict('k' => 1e3, 'm' => 1e6, 'b' => 1e9)

        suffix = str[end]
        number_str = str[1:end-1]

        number = parse(Float64, number_str)

        if haskey(multipliers, suffix)
            number *= multipliers[suffix]
        end
    catch
    end

    return number
end


function insert(contents::Array)
    try
        insert_many_contents("localhost", 27017, "letterbox_data", "movies", contents)
    catch
    end
end


function get_html(url::String)

    tries = 0
    headers = Dict("User-Agent"=> "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36")

    while tries < 5
        try
            response = HTTP.request("GET", url, headers)
            body = String(response.body)
            html = bs4(body)

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
