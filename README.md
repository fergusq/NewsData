# NewsData

## Riippuvuudet

Ubuntussa asenna pyppeteerin vaatimat riippuvuudet seuraavalla komennolla:

    sudo apt install gconf-service libasound2 libatk1.0-0 libatk-bridge2.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libexpat1 libfontconfig1 libgcc1 libgconf-2-4 libgdk-pixbuf2.0-0 libglib2.0-0 libgtk-3-0 libnspr4 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 ca-certificates fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils wget libcairo-gobject2 libxinerama1 libgtk2.0-0 libpangoft2-1.0-0 libthai0 libpixman-1-0 libxcb-render0 libharfbuzz0b libdatrie1 libgraphite2-3 libgbm1

Muut riippuvuudet voi asentaa pip-komennolla:

    pip install -r requirements.txt

### FiNER

FiNER-nimentunnistimen voi asentaa Dockerin avulla seuraavasti:

    cd FiNER
    docker build -t newsdata/finer .

Tämän jälkeen sen voi käynnistää näin:

    docker run --rm -it -p 19992:3000 --name finer newsdata/finer

## Esimerkkiskriptit

### Uutisten lataaminen

`example.py` lataa joukon uutisia `example.csv`-tiedostoon.

    python example.py

### Uutisten jäsentäminen

Uutisten jäsentämiseen voi käyttää Turun yliopiston suomen kielen jäsentäjää. Sen voi käynnistää dockerilla:

    docker run -d -p 15000:7689 turkunlp/turku-neural-parser:latest-fi-en-sv-cpu server fi_tdt parse_plaintext

Sanat voi jäsentää seuraavasti:

    cat example.csv | rq -v | jq -sr '.[] | .[6]' >/tmp/artikkelit.txt
    curl --request POST --header 'Content-Type: text/plain; charset=utf-8' --data-binary @/tmp/artikkelit.txt http://localhost:15000 >example-large.conllu

Jäsentäminen voi viedä kauan aikaa.

## Palvelin

### Tietokannan luominen

    python -m server.create_database

### Asetustiedosto

Asetustiedosto `config.ini` näyttää tältä:

    [Scraper]
    DatabaseURL = sqlite://./database.db

    [Parser]
    Enabled = yes
    URL = http://localhost:15000

    [FiNER]
    Enabled = yes
    URL = http://localhost:19992

    [hs.fi]
    Username = käyttäjä@esimerkki.fi
    Password = esimerkki

### Palvelimen käynnistäminen

Palvelimen lisäksi Turun yliopiston jäsennin pitää käynnistää kuten yllä.

    python -m server.main

Myös Annif-aihemallinnin pitää käynnistää (esim screenissä)
    
    cd KANSIO JOSSA ANNIF MALLI ON
    source ANACVONDAYMPÄRISTÖ
    annif run

serveri kannattaa käynnistää erillisessä screenissä, ettei serveri sammu serveriltä ulos kirjautuessa
    
    screen -S newsdata
    export http_proxy="http://gate102.vyh.fi:81"
    export https_proxy="http://gate102.vyh.fi:81"
    export no_proxy="127.0.0.1,localhost"

    cd KANSION JOSSA KOODIT OVAT
    source ANACONDAYMPÄRISTÖ
    PYPPETEER_CHROMIUM_REVISION=839947 python3 -m server.main