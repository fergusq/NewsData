import finer
from flask import Flask, request, Response
from flask_restful import inputs
import os
import json

app = Flask(__name__)

@app.route('/', methods=['POST', 'GET'])
def index():
    text = request.values.get("text")
    if text != None:
        print("request: "+ text)
        print(nertagger(text))

        result = ""

        show_version_param = request.values.get("showVersion")
        if show_version_param != "" and show_version_param != None:
            show_version = False
            try:
                show_version = inputs.boolean(show_version_param)
            except ValueError:
                print("Invalid value for parameter showVersion: " + show_version_param)
            if show_version == True:
                result += "FiNER, version " + os.environ['TAGTOOLS_VERSION'][1:] + "\n\n"

        result = nertagger(text)
        return Response(json.dumps(result), mimetype="application/json")
    else:
        return Response("Error - You should provide the input text as 'text' GET/POST parameter\n", status=500, mimetype="text/plain")
        
nertagger = finer.Finer("/app/finnish-tagtools/tag/") # pakollinen argumentti joka osoittaa FiNERin käyttämään datahakemistoon
print("FiNER ready and accepting connections.")
