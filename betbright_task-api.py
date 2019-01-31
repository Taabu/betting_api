# coding=utf-8
# __author__ = 'Mario Romera Fernández'

import flask
import db

app = flask.Flask(__name__)
# app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    """
    Handler for base url
    :return: Info html
    """

    return '''<h1>BetBright API by Mario Romera Fernández</h1>
<p>A prototype API for BetBright.</p>'''


@app.route('/api/v1/resources/external/', methods=['POST', 'PUT'])
def api_external_providers():
    """
    Handler for external data providers
    Accepts POST & PUT requests
    Handles 'NewEvent' & 'UpdateOdds' messages from external providers
    Checks for messages id to not process them if they have been already processed
    :return: Confirmation or deny html
    """

    query_parameters = flask.request.get_json(force=True)

    if not db.message_processed(query_parameters['id']):

        if query_parameters['message_type'] == 'NewEvent':
            db.new_event(query_parameters)

        elif query_parameters['message_type'] == 'UpdateOdds':
            db.update_odds(query_parameters)

        else:
            return "<h1>400</h1><p>Bad request: bad message_type</p>", 400

        return "<p>200</p>", 200

    return "<h1>400</h1><p>Message already processed</p>", 400


@app.route('/api/v1/resources/match/<int:match_id>', methods=['GET'])
def api_get_match_by_id(match_id):
    """
    Handler for get match by id
    Accepts GET requests
    :param match_id: INT the match id
    :return: JSON match info or html info
    """

    match_info = db.get_match_by_id(match_id)
    return ("<h1>404</h1><p>The resource could not be found.</p>", 404) if (
            len(match_info) == 0) else flask.jsonify(match_info)


@app.route('/api/v1/resources/match/', methods=['GET'])
def api_get_match_by_name():
    """
    Handler for get match by name
    Accepts GET requests
    :return: JSON match info or html info
    """

    query_parameters = flask.request.args
    if 'name' in query_parameters.keys():
        match_info = db.get_match_by_name(query_parameters['name'])
        return ("<h1>404</h1><p>The resource could not be found.</p>", 404) if (
            len(match_info) == 0) else flask.jsonify(match_info)

    return "<h1>400</h1><p>Bad request: bad argument</p>", 400


@app.route('/api/v1/resources/match/football/', methods=['GET'])
def api_match_football():
    """
    Handler for get matches ordered by start time
    Accepts GET requests
    :return: JSON matches info or html info
    """

    query_parameters = flask.request.args
    if 'ordering' in query_parameters.keys():
        if query_parameters['ordering'] == 'startTime':
            matches = db.get_football_matches_by_start_time()
            return flask.jsonify(matches)

    return "<h1>400</h1><p>Bad request: bad argument</p>", 400


@app.errorhandler(404)
def page_not_found():
    """
    Handler for page not found 404
    :return: html info
    """
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


if __name__ == '__main__':
    app.run()
