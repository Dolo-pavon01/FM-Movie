from re import search
from bs4 import BeautifulSoup
from time import sleep
from pymongo import MongoClient
from requests import session, Timeout, RequestException
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


PAGES_STEP = 100
COMMENTS_PAGES_LIMIT = 5 # max 12 comments by comments page


class Letterboxdcom:

    def __init__(self):

        self.db = 'testing'
        self.collection = 'collaborative_db_2'
        
        self.max_workers = 5
        self.base_url = "https://letterboxd.com"
        self.platform_code = "letterboxdcom"
        self.content_types = { "movie": [ "/films/ajax/popular/this/week/size/small/page/{}/?esiAllowFilters=true" ] }
        self.rating_url = "https://letterboxd.com/csi/film/{}/rating-histogram/"
        self.stats_url = "https://letterboxd.com/csi/film/{}/stats/"
        self.reviews_url = "https://letterboxd.com/ajax/{}/popular-reviews/"
        self.permalink = "https://letterboxd.com"
        self.created_at = datetime.now().strftime("%Y-%m-%d") # Returns string Ej "2024-06-28"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"
        }
        self.urls = set()
        self.crawlers_list = list()
        self.ids = set()
        self.items_to_load = list()
        self.session = session()

        self.mongo = MongoClient(host="localhost", port=27017)[self.db][self.collection]
        query = { "createdAt": self.created_at }
        projection = { "_id": 0, "id": 1 }
        self.contents_db = self.mongo.find(filter=query, projection=projection)
        self.contents_db = set( [ i["id"] for i in self.contents_db ] )
        
        print(f"\n{len(self.contents_db)} items already loaded.\n")

        self.process()

    def process(self):
        for content_type, content_urls in self.content_types.items():
            for content_url in content_urls:
                self.process_scraping(content_type=content_type, content_url=content_url)

    def process_scraping(self, content_type, content_url):
        self.crawlers_list.clear()
        url = self.base_url + content_url
        number_of_pages = self.get_page_numbers(url)
        print(f'\nThere are {number_of_pages} pages for {url}\n')
        for start in range(1, number_of_pages+1, PAGES_STEP):
            self.crawlers_list.clear()
            self.crawlers(url=content_url, pages_range=range(start, start+PAGES_STEP))
            self.scraping(content_type=content_type)

    def crawlers(self, url, pages_range):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.get_crawlers, self.base_url + url.format(page_num), page_num):
                    page_num for page_num in pages_range
            }

            for future in as_completed(futures):
                try:
                    crawlers_items = future.result()
                    if crawlers_items:
                        for crawler in crawlers_items:
                            if crawler.get('id') not in self.contents_db:
                                self.contents_db.add(crawler.get('id'))
                                self.crawlers_list.append(crawler)
                except Exception as e:
                    print(f'\nError inside scraping method: {e}')
    
    def get_crawlers(self, url, page_num):
        list_crawlers = list()

        soup = self._getSoup(url, headers=self.headers)
        if not soup:
            return None

        items = soup.find_all('li', {'class': 'listitem'})

        if items:
            for item in items:
                try:
                    div_info = item.div
                    if not div_info:
                        print('no div info')
                        continue

                    relative_link = div_info.get('data-target-link')
                    content_id = div_info.get('data-film-id')

                    if not (relative_link and content_id):
                        print('no relative_link or content_id')
                        continue

                    link = self.base_url + relative_link

                    try:
                        rating = float(item.get('data-average-rating'))
                    except:
                        rating = None

                except:
                    continue

                item_to_scrape = {
                    'url': link,
                    'id': content_id,
                    'rating': rating
                }
                list_crawlers.append(item_to_scrape)

            if (page_num % 100) == 0:
                print(f'Page: {page_num} - {len(list_crawlers)} items')

            return list_crawlers

        else:
            print(f"\n\nError getting crawlers from page {url}\n\n")
            return None

    def scraping(self, content_type):
        n = len(self.crawlers_list)
        print(f'\nTotal contents to scraping: {n}\n')
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.get_scraping, item, content_type, index+1, n):
                    (index,item) for (index,item) in enumerate(self.crawlers_list)
            }

            for future in as_completed(futures):
                try:
                    scraping_item = future.result()
                    if scraping_item:
                        self.items_to_load.append(scraping_item)
                        self.upload()
                except Exception as e:
                    print(f"\nError inside scraping method: {e}\n")

        self.insert()

    def get_scraping(self, item, content_type, index, n):

        # print(f"{index} / {n}".ljust(100) )
        print(f"{index} / {n}".ljust(100), end='\r')

        id_ = item['id']
        url = item['url']
        rating = item['rating']

        soup = self._getSoup(url, headers=self.headers)
        if not soup:
            return None
        if id_ not in self.ids:
            self.ids.add(id_)

            try:
                section_header = soup.find('section', {'class': 'film-header-group'})
                # Get title
                try:
                    h1_title = section_header.find('h1')
                    title = h1_title.text.strip()
                except:
                    print('NO TITLE')
                    return None

                # Get year
                # try:
                #     year = section_header.select_one('a[href*="/films/year/"]')
                #     year = validated_year(year.text)
                # except:
                #     year = None

                # Get imdb/tmdb id
                imdb_id, tmdb_id = None, None
                externalIds = []
                a_tag_imdb = soup.find('a', {'data-track-action': 'IMDb'})
                if a_tag_imdb and a_tag_imdb.get('href'):
                    imdb_link = a_tag_imdb.get('href')
                    imdb_id = search('tt\d+', imdb_link).group()
                    imdb = {'Provider': 'imdb', 'Id': imdb_id}
                    externalIds.append(imdb)
                a_tag_tmdb = soup.find('a', {'data-track-action': 'TMDb'})
                if a_tag_tmdb and a_tag_tmdb.get('href'):
                    tmdb_link = a_tag_tmdb.get('href')
                    tmdb_id = tmdb_link.split('/')[-2]
                    externalIds.append({'Provider': 'tmdb', 'Id': tmdb_id})
                if not externalIds:
                    externalIds = None

                # Get votes, views, likes
                relative_link = url.split('/')[-2]
                votes = self.get_votes(relative_link)
                views, likes = self.get_views_likes(relative_link)

                comments = self.get_comments(url)

                payload_item = {
                    "platformCode": self.platform_code,
                    "id": id_,
                    "permalink": url,
                    "title": title,
                    "type": content_type,
                    
                    "comments": comments,

                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    
                    "popularity": {
                        "votes": votes,
                        "likes": likes,
                        "views": views,
                        "rating": rating,
                    },
                    
                    "createdAt": self.created_at,
                }
                
                return payload_item

            except Exception as e:
                print(f"\nError scraping {url} - \n{e}\n")
                return None

    def get_page_numbers(self, url):

        total_pages = 1

        try:
            soup = self._getSoup(url.format(1))
            if not soup:
                raise Exception(f"No soup object for url: '{url.format(1)}'.")
        except Exception as e:
            return total_pages

        try:
            movies_raw_number = soup.find("p", class_="ui-block-heading").text
            total_movies = int("".join(filter(str.isdigit, movies_raw_number)))
            movies = soup.find_all('li', {'class': 'listitem'})

            total_pages = int(total_movies / len(movies))

            last_page = False
            while not last_page:
                total_pages += 1
                try:
                    soup = self._getSoup(url.format(total_pages))
                    movies = soup.find_all('li', {'class': 'listitem'})
                    last_page = len(movies) < 72
                except Exception as e:
                    last_page = True
        except Exception as e:
            pass

        return total_pages

    def get_votes(self, relative_link):

        votes = 0

        try:
            url = self.rating_url.format(relative_link)
            soup = self._getSoup(url)
            if not soup:
                raise ConnectionError

            fans_tag = soup.select_one('a[href*="/fans/"]')
            if fans_tag:
                votes = fans_tag.text.replace(' fans', '')
                votes = self.metric_converter(votes)
        except Exception as e:
            print(e)
            pass

        return votes

    def get_views_likes(self, relative_link):

        views, likes = 0, 0

        try:
            url = self.stats_url.format(relative_link)
            soup = self._getSoup(url)
            if not soup:
                raise ConnectionError

            li_views = soup.find('li', {'class': 'stat filmstat-watches'})
            if li_views:
                views = self.metric_converter(li_views.text)

            li_likes = soup.find('li', {'class': 'stat filmstat-likes'})
            if li_likes:
                likes = self.metric_converter(li_likes.text)
        except Exception as e:
            print(e)
            pass
        return views, likes

    def get_comments(self, url):
        i = 1
        movie_reviews = []
        while (i <= COMMENTS_PAGES_LIMIT):
            tries = 2
            while tries > 0:
                try:
                    urlReviews = url + f'reviews/page/{i}'
                    soup = self._getSoup(urlReviews)
                except Exception as e:
                    print(f"Error de conexion - Durmiendo por 2 segundos... quedan {tries} intentos mas para conseguir los comentarios de {urlReviews}")
                    sleep(2)
                    tries -= 1
                    if tries == 0:
                        print(f"Error de conexion - Durmiendo por 2 segundos... quedan {tries} intentos mas para el titulo {urlReviews}")
                else:
                    try: 
                        all_reviews = soup.find_all("li", class_="film-detail")

                    except Exception as e:
                        pass
                    else:
                        for review in all_reviews:
                            review_text = None
                            try:
                                # date = review.find('span', class_='_nobr').text
                                review2 = review.find('div', class_='body-text -prose collapsible-text')
                                if review2.find('p', class_="contains-spoilers"):
                                    review_text = review2.find('div', 'hidden-spoilers expanded-text').text
                                else:
                                    review_text = review2.text
                            except Exception as e:
                                pass
                            # res = {'review': review_text, 'date': date,'urlReviews': urlReviews}
                            movie_reviews.append(review_text)
                        i += 1
                        break

        return movie_reviews

    def metric_converter(self, metric):
        multipliers = {'k': 1e3, 'K': 1e3, 'm': 1e6, 'M': 1e6, 'b': 1e9, 'B': 1e9}

        def converter(x): return int(float(x[:-1])*multipliers[x[-1]])

        try:
            if metric.endswith(('k', 'm', 'b', 'K', 'M', 'B')):
                metric = converter(metric)
            else:
                metric = int(metric)
        except:
            metric = None

        return metric

    def upload(self):
        if len(self.items_to_load) >= 100:
            self.insert()
    
    def insert(self):
        try:
            self.mongo.insert_many(documents=self.items_to_load, ordered=False)
            self.items_to_load.clear()
        except Exception as e:
            print(f"\n\nERROR INSERTING DATA IN MONGO:\n{str(e)[:500]}\n\n")

    def _getSoup(self, url=None, method='get', headers=None, cookies=None, data=None, params=None, timeout=None, max_attemps=5):
        
        """
        Public method \n
        Genera el object soup mediante una url y headers (si es necesario).

        - Args:
            - url (str)
            - method (str) Default [get]
            - headers (dict) or None
            - cookies (dict) or None
            - data (dict) or None
            - params (dict) or None
            - timeout (int) or None
            - max_attemps (int) Default [5]
        - Returns:
            - soup (object) or None
            """

        attemps = 1
        while attemps <= max_attemps:
            try:
                if method == 'get':
                    response = self.session.get(url=url, 
                                                headers=headers, 
                                                cookies=cookies, 
                                                data=data, 
                                                params=params, 
                                                timeout=timeout)
                elif method == 'post':
                    response = self.session.post(url=url, 
                                                    headers=headers, 
                                                    cookies=cookies, 
                                                    data=data, 
                                                    params=params, 
                                                    timeout=timeout)
                else:
                    raise Exception(f'Debe especificar get o post, {method} no es válido.')

                if response.status_code != 200:
                    attemps += 1
                    sleep(5)
                    continue

                soup = BeautifulSoup(response.text, features="html.parser")
                return soup
            except Timeout as e:
                print(f'{e} -> {url}')
                return None
            except (Exception, ConnectionError, RequestException) as e:
                attemps += 1
                print(f'{e} -> Intento número {attemps}.')
                sleep(5)
                continue

        if attemps > 5:
            print(f'No se pudo acceder a la URL -> {url}')
            return None


if __name__ == '__main__':
        
    Letterboxdcom()

