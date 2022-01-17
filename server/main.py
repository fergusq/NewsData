import asyncio
import configparser
import datetime
import io
import logging
import re
import uuid
from typing import Any
import json

import databases
import matplotlib.pyplot as plt
import pandas as pd
from aiohttp import web
from scrapers import query

from server.analysis import analyze, analyze_tweets
from server.scraping import start_scraping, start_scraping_twitter
import server.scheduler as scheduler

logging.basicConfig(filename='server.log', level=logging.INFO)

routes = web.RouteTableDef()

@routes.post("/scrape_twitter")
async def scrape_twitter(request: web.Request):
    try:
        params = await request.json()
    
    except json.JSONDecodeError:
        logging.warning("Malformed JSON request: " + await request.text())
        raise web.HTTPBadRequest()

    if not isinstance(params, dict) or {"accounts","from_date","to_date"} < set(params):
        raise web.HTTPBadRequest()
    
    if not isinstance(params["from_date"], str):
        raise web.HTTPBadRequest()
    
    if not isinstance(params["to_date"], str):
        raise web.HTTPBadRequest()
    
    if not isinstance(params["accounts"], list):
        raise web.HTTPBadRequest()
    
    try:
        from_date = datetime.datetime.fromisoformat(params["from_date"])
        to_date = datetime.datetime.fromisoformat(params["to_date"])
    
    except:
        logging.warning(f"Malformed dates: {params['from_date']}, {params['to_date']}")
        raise web.HTTPBadRequest()

    ticket_id = str(uuid.uuid4())
    db: databases.Database = request.app["db"]
    await db.execute("INSERT INTO tickets(uuid, status, date) VALUES (:id, :status, datetime('now'));", {"id": ticket_id, "status": "in progress"})

    response = web.json_response({"status": "ok", "message": "Scrape scheluded!", "ticket_id": ticket_id})
    await response.prepare(request)
    await response.write_eof()

    asyncio.create_task(start_scraping_twitter(params["accounts"], from_date, to_date, ticket_id, request.app), name="scraper")
    return response

@routes.post("/scrape")
@routes.post("/analyse")
async def scrape(request: web.Request):
    try:
        params = await request.json()
    
    except json.JSONDecodeError:
        logging.warning("Malformed JSON request: " + await request.text())
        raise web.HTTPBadRequest(reason="Json parse error")

    required = {"query", "from_date", "media", "enabled"}
    if not isinstance(params, dict) or required - set(params):
        raise web.HTTPBadRequest(reason="Missing required parameters: " + str(required - set(params)))
    
    if not re.fullmatch(r"\d\d\d\d-\d\d-\d\d", params["from_date"]):
        raise web.HTTPBadRequest(reason="Malformed from_date")
    
    if "to_date" in params and not re.fullmatch(r"\d\d\d\d-\d\d-\d\d", params["to_date"]):
        raise web.HTTPBadRequest(reason="Malformed to_date")
    
    if not isinstance(params["media"], list):
        raise web.HTTPBadRequest(reason="media is not a list")
    
    if not isinstance(params["enabled"], list):
        raise web.HTTPBadRequest(reason="enabled is not a list")
    
    query_params = query.Params(
        query=params["query"],
        from_date=datetime.date.fromisoformat(params["from_date"]),
        to_date=datetime.date.fromisoformat(params.get("to_date", None)),
        enabled=params["enabled"],
        extra=params.get("params", {})
    )
    media = params["media"]

    ticket_id = str(uuid.uuid4())
    db: databases.Database = request.app["db"]
    await db.execute("INSERT INTO tickets(uuid, status, date) VALUES (:id, :status, datetime('now'));", {"id": ticket_id, "status": "in progress"})

    response = web.json_response({"status": "ok", "message": "Scrape scheluded!", "ticket_id": ticket_id})
    await response.prepare(request)
    await response.write_eof()

    asyncio.create_task(start_scraping(query_params, media, ticket_id, request.app), name="scraper")
    return response

@routes.get("/ticket/{uuid}")
async def get_ticket(request: web.Request):
    ticket_id = request.match_info["uuid"]
    db: databases.Database = request.app["db"]
    row = await db.fetch_one("SELECT status, resource_id FROM tickets WHERE uuid = :id;", {"id": ticket_id})
    if not row:
        raise web.HTTPNotFound()
    
    return web.json_response({"status": row["status"], "resource_id": row["resource_id"], "ticket_id": ticket_id})

@routes.get("/resource/{uuid}")
async def get_resource(request: web.Request):
    resource_id = request.match_info["uuid"]
    db: databases.Database = request.app["db"]
    row = await db.fetch_one("SELECT resource FROM resources WHERE uuid = :id;", {"id": resource_id})
    if not row:
        raise web.HTTPNotFound()
    
    return web.Response(body=row["resource"])

@routes.get("/resource/{uuid}/analysis/{method}")
async def analysis(request: web.Request):
    resource_id = request.match_info["uuid"]
    method = request.match_info["method"]

    if resource_id == "twitter":
        response = analyze_tweets(method, request.query)
    
    else:
        db: databases.Database = request.app["db"]
        row = await db.fetch_one("SELECT resource FROM resources WHERE uuid = :id;", {"id": resource_id})
        if not row:
            raise web.HTTPNotFound()
        
        data: Any = pd.read_csv(io.StringIO(row["resource"]))
        response = analyze(data, method, request.query)

    if request.query.get("index", None):
        response = response.set_index(request.query["index"]).sort_index()

    if "columns" in request.query:
        columns = request.query["columns"].split(",")
        response = response[columns]
    
    if "groupby" in request.query:
        rule = request.query.get("groupby")
        groupers: list = []
        for r in rule.split(","):
            if r[0].isnumeric():
                groupers.append(pd.Grouper(freq=r))
            
            else:
                groupers.append(pd.Grouper(key=r))
        
        groupby = response.groupby(groupers)
        
        agg = request.query.get("aggregate", "sum")
        if agg == "sum":
            response = groupby.sum()
        
        elif agg == "mean":
            response = groupby.mean()
        
        elif agg == "median":
            response = groupby.median()
        
        elif agg == "std":
            response = groupby.std()
        
        elif agg == "var":
            response = groupby.var()
        
        elif agg == "min":
            response = groupby.min()
        
        elif agg == "min":
            response = groupby.max()
        
        elif agg == "size":
            response = pd.DataFrame({"count": groupby.size()})
        
        else:
            response = groupby.sum()
        
        if agg != "size":
            response["count"] = groupby.size()

    if "sort_key" in request.query:
        response = response.sort_values(request.query["sort_key"], ascending=False)
    
    format = request.query.get("format", "csv")
    if format == "csv":
        return web.Response(body=response.to_csv().encode("utf-8"), content_type="text/csv", charset="utf-8")
    
    elif format == "json":
        return web.Response(body=response.to_json().encode("utf-8"), content_type="application/json", charset="utf-8")

    elif format == "html":
        return web.Response(body=response.to_html().encode("utf-8"), content_type="text/html", charset="utf-8")

    elif format == "png":
        plottype = request.query.get("plot", "line")

        data = io.BytesIO()
        if plottype == "line":
            response.plot()
        
        elif plottype == "bar":
            response.plot.bar()
        
        plt.savefig(data)
        return web.Response(body=data.getvalue(), content_type="image/png")
    
    else:
        raise web.HTTPBadRequest(reason="Illegal format parameter value")

@routes.post("/run_daily_schedule")
async def run_daily_schedule(request: web.Request):
    asyncio.create_task(scheduler.run_daily_schedule(request.app), name="daily schedule")
    return web.Response(status=200)

async def init_app():
    parser = configparser.ConfigParser()
    parser.read("config.ini")

    db = databases.Database(parser["Scraper"].get("DatabaseURL", "sqlite://./database.db"))
    await db.execute("UPDATE tickets SET status = 'interrupted' WHERE status = 'in progress';")
    app = web.Application()
    app["db"] = db
    app["PARSER_URL"] = parser["Parser"].get("URL", "http://localhost:15000")
    app["PARSER_ENABLED"] = parser["Parser"].getboolean("Enabled")
    app["NER_URL"] = parser["FiNER"].get("URL", "http://localhost:19992")
    app["NER_ENABLED"] = parser["FiNER"].getboolean("Enabled")
    app["TWITTER_BEARER"] = parser["twitter.com"].get("Bearer")
    app["TWITTER_ENABLED"] = parser["twitter.com"].getboolean("Enabled")
    app["TWITTER_METADATA"] = parser["twitter.com"].get("UserMetadata", None)
    app["ANNIF_URL"] = parser["Annif"].get("URL", "http://127.0.0.1:5000/v1/projects/yle-2021-ensemble-fi/suggest")
    app["ANNIF_ENABLED"] = parser["Annif"].getboolean("Enabled")
    app["HS_USERNAME"] = parser["hs.fi"]["Username"]
    app["HS_PASSWORD"] = parser["hs.fi"]["Password"]
    
    app.add_routes(routes)
    return app

web.run_app(init_app())
