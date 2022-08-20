# IMPORTS
from urllib.request import urlopen
from bs4 import BeautifulSoup
from random import randint
import concurrent.futures
import pymongo
import json

# Initialize variables
data = {}

# Read file
login_info = json.load(open("login.json"))

# Get login info from login file
ADDRESS = login_info["address"]
PORT = login_info["port"]
USERNAME = login_info["username"]
PASSWORD = login_info["password"]

# Connect to MongoDB, whilst checking if the login file specified localhost
if ADDRESS.lower() == "localhost":
    client = pymongo.MongoClient(f"mongodb://localhost/")
else:
    client = pymongo.MongoClient(f"mongodb://{USERNAME}:{PASSWORD}@{ADDRESS}:{PORT}/")

# Get all databases and the targeted collection
db = client['VIVY']
col = db['liederCOL']

# # COMMENT OR DELETE
# col.drop()
# col = db['liederTestDB']

# # Check if the database is not empty
if col.count_documents({}):
    print(col.find_one(sort=[('_id', -1)])['_id'])
    # Get highest ID from the database
    high = col.find_one(sort=[('_id', -1)])['_id'] + 1
else:
    # Set high to 0
    high = 1

# Main scraping method
def scrape(song_id):
    
    # Try
    try:
    
        # Variable initialization
        key = "text"
        verification = False

        # Get readable information of a website URL
        html = urlopen('https://www.lieder.net/lieder/get_text.html?TextId=' + str(song_id)).read().decode('utf-8', 'ignore')
        soup = BeautifulSoup(html, 'lxml')
        text = soup.find('div', class_='text-table')
        
        # Check if the text is tr only
        key = 'tr' if soup.find('div', class_='title text-title') is None else key

        #
        #   POEM TITLE
        #


        # Get title of the poem
        poem_name = text.find('div', class_=f'title {key}-title')

        #
        #   POEM TEXT
        #


        # Get the text of the poem
        poem_text = text.find('div', class_=f'text the-{key}')

        #
        #   POEM LANGUAGE
        #


        # Get language of poem
        lang = text.find('div', class_=f'lang {key}-lang detail')
        poem_lang = lang.text.replace("Language:", "").split()[0]

        #
        #   POEM TRANSLATION
        #


        # Get information of the poem translations
        poem_tr = text.find('div', class_=f'trs {key}-trs detail')

        # Check if there is any translations
        if poem_tr is None: 

            # If not, set trans to None
            trans = None

        # Check if not
        else:

            # If so, get the list of translations
            trans = poem_tr.text.split()[2:]

        #
        #   POEM AUTHOR ID WITH VERIFICATION STATUS
        #


        # Get list of all information within the poem's info 
        list_info = text.find('div', class_=f'notes {key}-notes detail')

        # Get all "a" HTML elements of this list of information
        author_info = list_info.find_all(lambda tag: tag.name == 'a' and ("?AuthorId" in tag.get('href') if tag.get('href') else False))

        # try
        try: 
            
            # Attempt to get the ID of the author
            id = author_info[0].get('href').split("=")[-1]
            
            # Get author's lieder.net page and information
            author_html = urlopen('https://www.lieder.net/lieder/get_author_texts.html?AuthorId=' + str(id)).read().decode('utf-8', 'ignore')
            author_soup = BeautifulSoup(author_html, 'lxml')
            
            # Get author name
            author_text = author_soup.find('fieldset').find('h2').text
            
            # Get verification status of the author
            verified = True if "author's text checked" in author_info[0].parent.text else False
        
        # Except
        except:
            
            # If failed to do so, set id to Anonymous
            author_text = 'Anonymous'
        
            # Set verification status to False
            verified = False

        # Put information to the poet information variable
        poem_poet = [author_text, verified]

        #
        #   PAIRED SONGS TO POEM
        #

        # Create a variable for 
        songs = []

        # Initialize song variable
        songs = []

        # Get all "a" HTML elements of this list of information that has a ComposerID href with specific text
        song_list = list_info.find_all(lambda tag: tag.name == 'a' and 
                                    ("?ComposerId" in tag.get('href') if tag.get('href') else False) and
                                    ("text" in tag.parent.text))
        
        # Loop through list of specific "a" HTML elements
        for tag in song_list:
            
            # Initialize variables
            status = False
            
            # Form a list from the tag's siblings generator
            text_list = ' '.join([i.text for i in list(tag.next_siblings)])
            
            # Generate song name from text_list
            song_name = text_list.split('"')[1::2][0]
            
            # Parse the composer id of the current music
            id = tag.get('href').split("=")[-1]
            
            # Get composer's lieder.net page and information
            composer_html = urlopen('https://www.lieder.net/lieder/get_settings.html?ComposerId=' + str(id)).read().decode('utf-8', 'ignore')
            composer_soup = BeautifulSoup(composer_html, 'lxml')
            
            # Get author name
            composer_text = composer_soup.find('fieldset').find('h2').text
            
            # Check if the line has spans
            if tag.parent.find('span'):
                # Get main attribute 
                attributes = list(tag.parent.find('span').attrs.keys())
                attribute = [attr for attr in attributes if attr == 'style' or attr == 'class'][0]
                
                # Check if the song is verified and if som
                # Set status to true
                if tag.parent.find('span')[attribute][0].upper() == 'VERIFIED' or 'green' in tag.parent.find('span')[attribute]:
                    status = True
                    verification = True
            # If not spans, look into divs
            else:
                # Get main attribute 
                attributes = list(tag.parent.find('div').attrs.keys())
                attribute = [attr for attr in attributes if attr == 'style' or attr == 'class'][0]
                
                # Check if the song is verified and if som
                # Set status to true
                if 'green' in tag.parent.find('div')[attribute] or tag.parent.find('div')[attribute][0].upper() == 'VERIFIED':
                    status = True
                    verification = True
                
            # Compile all parsed results and append it to songs list
            songs.append([song_name, composer_text, status])

        # Add gathered information to data dictionary
        data = {
            "_id": song_id,
            "poem_name": poem_name.text,
            "poem_text": poem_text.text,
            "poem_lang": poem_lang,
            "poem_tr": trans,
            "poem_poet": poem_poet,
            "songs": songs
        }

        # Insert document to collections
        col.insert_one(data)
        
        # Print status
        print(str(song_id) + " added | " + ("Verified" if verification else "Not Verified"))
        
    # Except
    except Exception as e:
        # Print status
        print(str(song_id) + " not added")
        # traceback.print_exc()
        # print("\n")
        
        # # Log error
        # with open(f'logs/{song_id}.txt', 'w') as file:
        #     traceback.print_exc(file=file)
            
# Main process
if __name__ == '__main__':
    # MultiThreading 
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Run scraper method concurrently
        _ = [executor.submit(scrape, i) for i in range(high, 400000)]
