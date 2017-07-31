import bs4 as bs
import urllib.request

sauce = urllib.request.urlopen('https://dacatay.com').read()
soup = bs.BeautifulSoup(sauce, 'lxml')

#print(soup.prettify())

#for url in soup.find_all('a'):
#    print(url.get('href'))


nav = soup.nav
#for url in nav.find_all('a'):
#    print(url.get('href'))


body = soup.body
for paragraph in body.find_all('p'):
    print(paragraph.text)


for div in soup.find_all('div', class_='body'):
    print(div.text)
