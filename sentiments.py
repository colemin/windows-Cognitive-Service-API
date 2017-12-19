import pandas as pd
import json
import httplib, urllib
import codecs
import time

# **********************************************
# *** Variables Cognitive API Microsoft      ***
# **********************************************
accessKey = 'c1da5ae95d3d4ff487ea31a4fc529f56'
uri = 'westcentralus.api.cognitive.microsoft.com'
headers = {
    # Request headers
    'Content-Type': 'application/json',
    'Ocp-Apim-Subscription-Key': accessKey,
}
params = urllib.urlencode ({
    # Request parameters
    'numberOfLanguagesToDetect': '3',
})

limit = 200


def GetLanguage (documents):
    "Detects the languages for a set of documents and returns the information."

    path = '/text/analytics/v2.0/languages'
    headers = {'Ocp-Apim-Subscription-Key': accessKey}
    conn = httplib.HTTPSConnection (uri)
    body = json.dumps (documents, ensure_ascii=False)
    conn.request ("POST", path, body, headers)
    response = conn.getresponse ()
    return(response.read ())

def GetSentiment (documents):
    "Gets the sentiments for a set of documents and returns the information."

    path = '/text/analytics/v2.0/sentiment'
    headers = {'Ocp-Apim-Subscription-Key': accessKey}
    conn = httplib.HTTPSConnection (uri)
    body = json.dumps (documents)
    conn.request ("POST", path, body, headers)
    response = conn.getresponse ()
    return response.read ()


def GetKeyPhrases (documents):
    "Gets the sentiments for a set of documents and returns the information."

    path = '/text/analytics/v2.0/keyPhrases'
    headers = {'Ocp-Apim-Subscription-Key': accessKey}
    conn = httplib.HTTPSConnection (uri)
    body = json.dumps (documents)
    conn.request ("POST", path, body, headers)
    response = conn.getresponse ()
    return response.read ()


def saveJSONfile1(input, num, type):
    with codecs.open ('../Sentiment/tgt/comments/block_{}_{}.json'.format (num,type), encoding='utf-8', mode='w+') as outfile:
        outfile.write (json.dumps (input, ensure_ascii=False, encoding='utf8', indent=2))
        outfile.close ()

def saveJSONfile(input, num, type, path):
    if num == '':
        with codecs.open ('{}/out_{}.json'.format (path,type), encoding='utf-8', mode='w+') as outfile:
            outfile.write (json.dumps (input, ensure_ascii=False, encoding='utf8', indent=2))
            outfile.close ()
    else:
        with codecs.open ('{}/block_{}_{}.json'.format (path,num,type), encoding='utf-8', mode='w+') as outfile:
            outfile.write (json.dumps (input, ensure_ascii=False, encoding='utf8', indent=2))
            outfile.close ()

def addLang(dataBlock, json_out_API_GetLanguage, data):

    for item in json_out_API_GetLanguage['documents']:
        for i in range (len (dataBlock['documents'])):
            if dataBlock['documents'][i]['id'] == item['id']:
                dataBlock['documents'][i]['language'] = item['detectedLanguages'][0]['iso6391Name']
                data['documents'][i]['language'] = item['detectedLanguages'][0]['iso6391Name']
                data['documents'][i]['langName'] = item['detectedLanguages'][0]['name']
                data['documents'][i]['langScore'] = item['detectedLanguages'][0]['score']

def addSentiment(json_out_API_GetSentiment, data):
    #Para cada ID busco en mi esctructura Data para anadir los campos del lenguage
    for item in json_out_API_GetSentiment['documents']:
        for i in range (len (data['documents'])):
            if data['documents'][i]['id'] == item['id']:
                data['documents'][i]['sentimentScore'] = item['score']

def addKeyPhrases (json_out_API_GetKeyPhrases, data):
    # Para cada ID busco en mi esctructura Data para anadir los campos del lenguage
    for item in json_out_API_GetKeyPhrases['documents']:
        for i in range (len (data['documents'])):
            if data['documents'][i]['id'] == item['id']:
                data['documents'][i]['keyPhrases'] = item['keyPhrases']


def processBlock(dataBlock, data, num, path):

                saveJSONfile(dataBlock, num, 'IN', path)

                json_out_API_GetLanguage = json.loads (GetLanguage (dataBlock))
                #saveJSONfile1(json_out_API_GetLanguage, num, 'LANGUAGE')
                saveJSONfile(json_out_API_GetLanguage, num, 'LANGUAGE', path)
                addLang(dataBlock, json_out_API_GetLanguage, data)

                json_out_API_GetSentiment = json.loads (GetSentiment (dataBlock))
                #saveJSONfile1(json_out_API_GetSentiment, num, 'SENTIMENT')
                saveJSONfile(json_out_API_GetSentiment, num, 'SENTIMENT', path)
                addSentiment(json_out_API_GetSentiment, data)

                json_out_API_GetKeyPhrases = json.loads (GetKeyPhrases (dataBlock))
                #saveJSONfile1(json_out_API_GetKeyPhrases, num, 'KEYWORDS')
                saveJSONfile(json_out_API_GetKeyPhrases, num, 'KEYWORDS', path)
                addKeyPhrases(json_out_API_GetKeyPhrases, data)


if __name__ == "__main__":

    typePost = raw_input ("\nQue quieres procesar:[posts / comments / responses] ")
    path = '../Sentiment/tgt/{}'.format (typePost)

    print('\n---------------------\n')


    data = {}
    data['documents'] = []

    # Filtramos todos los feeds de vueling y los que esten vacios.
    csv_file = pd.DataFrame(
        pd.read_csv("../Sentiment/src/vueling_facebook_{}.csv".format(typePost), sep = ",", header = 0, index_col = False, na_filter = False))\
        .query('from_name != "Vueling" & message != ""')[['id', 'message']]

    dataBlock = {}
    dataBlock['documents'] = []
    num = -1
    for index, row in csv_file.iterrows ():
        if index%limit == 0:
            num += 1
            if len(dataBlock['documents'])>0:

                print(' Estamos procesado el bloque {} de {} {}.'.format(num, len(dataBlock['documents']),typePost))
                processBlock(dataBlock, data, num, path)
                time.sleep(5)

                dataBlock = {}
                dataBlock['documents'] = []

        dataBlock['documents'].append ({'id': row['id'], 'text': row['message']})
        data['documents'].append ({'id': row['id'], 'text': row['message']})

    print(' Estamos procesado el bloque {} de {} {}.\n\n HEMOS TERMINADO!\n'.format (num+1, len (dataBlock['documents']), typePost))

    processBlock(dataBlock, data, num+1, path)

    saveJSONfile(data,'',typePost,'../Sentiment/tgt/')

