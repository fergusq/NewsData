# Skreippaus- ja analysointipalvelimen rajapinta

## POST `/scrape_twitter`

Syötteen muoto:

```json
{
    "from_date": "",
    "to_date": "",
    "accounts": [""]
}
```

* **`"from_date"`** _(pakollinen)_ hakuvälin alku (päivämäärä ja valinnainen kellonaika)
* **`"to_date"`** _(pakollinen)_ hakuvälin loppu (päivämäärä ja valinnainen kellonaika)
* **`"accounts"`** _(pakollinen)_ haettavat Twitter-tunnukset (lista merkkijonoja)

Syöte-esimerkki:

```json
{
    "from_date": "2021-05-10",
    "to_date": "2021-06-15",
    "accounts": ["esimerkki1", "esimerkki2"]
}
```

Palaute:

```json
{"status": "ok", "message": "Scrape scheluded!", "ticket_id": "71c1d873-d8d0-4e32-a932-52846b5da2d6"}
```

## POST `/analyse`

Syötteen muoto:

```json
{
    "query": "",
    "from_date": "",
    "to_date": "",
    "media": [""],
    "enabled": [""],
    "params": {}
}
```

* **`"query"`** _(pakollinen)_ hakukysely (merkkijono)
* **`"from_date"`** _(pakollinen)_ hakuvälin alku (päivämäärä ja valinnainen kellonaika)
* **`"to_date"`** _(valinnainen)_ hakuvälin loppu (päivämäärä ja valinnainen kellonaika)
* **`"media"`** _(pakollinen)_ tutkittavat mediat (lista merkkijonoja, vaihtoehdot: `"hs"`, `"is"`, `"il"`, `"yle"`, `"twitter"`)
* **`"enabled"`** _(pakollinen)_ käytössä olevat työvaiheet (lista merkkijonoja, vaihtoehdot: `"content"`, `"annif"`, `"ner"`, `"parser"`, `"twitter"`)
* **`"params"`** _(valinnainen)_ eri skreippereiden omia parametreja (tällä hetkellä vain Twitter-työkalu käyttää tätä)
  * **`"scrape_ids"`** _(valinnainen)_ niiden skreippausten (`/scrape_twitter`-komennolla luodut) id:t, jotka otetaan mukaan analyysiin (lista merkkijonoja)
  * **`"sample"`** _(valinnainen)_ otoskoko Twitter-viestien analysointia varten (kokonaisluku)
  * **`"accounts"`** _(valinnainen)_ haettavat Twitter-käyttäjät (lista merkkijonoja)
  * **`"drop_retweets"`** _(valinnainen)_ poista uudelleentviittaukset aineistosta (totuusarvo)

Syöte-esimerkki:

```json
{
    "query": "ilmasto",
    "from_date": "2021-05-10",
    "to_date": "2021-06-15",
    "media": ["yle", "twitter"],
    "enabled": ["content", "annif", "ner"],
    "params": {
        "scrape_ids": ["71c1d873-d8d0-4e32-a932-52846b5da2d6"],
        "sample": 1000,
        "accounts": ["esimerkki1", "esimerkki2"],
    }
}
```

Palaute:

```json
{"status": "ok", "message": "Scrape scheluded!", "ticket_id": "a58856a6-9de7-48c5-aaef-7a83a14d24f8"}
```

## GET `/ticket/{id}`

Palauttaa toimeksiannon tilan.

Palaute `/scrape_twitter`-toimeksiannolle:

```json
{"status": "finished", "resource_id": null, "ticket_id": "71c1d873-d8d0-4e32-a932-52846b5da2d6"}
```

Palaute `/analyse`-toimeksiannolle:

```json
{"status": "finished", "resource_id": "a58856a6-9de7-48c5-aaef-7a83a14d24f8", "ticket_id": "a58856a6-9de7-48c5-aaef-7a83a14d24f8"}
```

## GET `/resource/{id}`

Palauttaa koko `/analyse`-komennolla luodun datan.

## GET `/resource/{id}/analysis/{method}`

Palauttaa yhteenvedon datasta.
