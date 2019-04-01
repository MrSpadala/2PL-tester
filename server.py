
from flask import Flask
from flask import request

from check2PL import solve2PL
from checkConflict import solveConflict
from utils import parse_schedule

app = Flask(__name__)


index_cached = open('index.html', 'r').read()


@app.route("/2PL", methods=['GET'])
def index():
    schedule = request.args.get('schedule')

    response = index_cached

    if schedule is None:
        return response

    # stupid check
    tmp = map(lambda c: ord(c)<=31 or ord(c)==127, schedule)
    if any(tmp):
        return "<===3"

    schedule = schedule.replace(' ', '')
    if schedule == '':
        return format_response('Empty schedule', response)

    sched_parsed = parse_schedule(schedule)
    print(sched_parsed)

    if type(sched_parsed) == str:  #parsing error message
        return format_response('Parsing error: '+sched_parsed, response)
    

    # Solve
    res2PL = solve2PL(sched_parsed)
    resConfl = solveConflict(sched_parsed)


    # Format results for conflict serializability
    msg = '<b><i>Conflict serializability</i></b><br>'
    msg += 'Is the schedule conflict serializable: <i>'+str(resConfl)+'</i>'
    response = format_response(msg, response)


    # Format results for 2PL
    msg = '<b><i>Two phase lock protocol</i></b><br>'
    if res2PL['sol'] is None:
        #return format_response('<br>'+res2PL['err']+'<br><br>'+res2PL['partial_locks'])
        msg += res2PL['err']+'<br>'
        response = format_response(msg, response)

    else:
        msg += """
        Solution: {}, <br>
        Is the schedule strict-2PL: <i>{}</i>, <br>
        Is the schedule strong strict-2PL: <i>{}</i>
        """.format(res2PL['sol'], res2PL['strict'], res2PL['strong'])
        response = format_response(msg, response)

    return response


@app.route('/manager/html', methods=['GET'])
def go_away():
    return '<===3'


def format_response(msg, res):    
    return res.replace('<!---->', '<br>'+msg+'<br><!---->')



if __name__ == "__main__":
    from os.path import isfile
    debug = isfile('.DEBUG')
    app.run(host="0.0.0.0", port=8080, debug=debug)
