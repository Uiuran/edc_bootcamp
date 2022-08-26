import wikipediaapi
headers = {'User-Agent':'http'}
wiki = wikipediaapi.Wikipedia(language='en',headers=headers)

article = wiki.article('Buddhism')

print(article.summary)

print(article.text)