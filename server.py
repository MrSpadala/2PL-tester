
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

    if schedule is None:
        return index_cached

    # stupid check
    tmp = map(lambda c: ord(c)<=31 or ord(c)==127, schedule)
    if any(tmp):
        return "<===3"

    schedule = schedule.replace(' ', '')
    if schedule == '':
        return format_response('<br>Empty schedule')

    sched_parsed = parse_schedule(schedule)
    print(sched_parsed)

    if type(sched_parsed) == str:  #error message
        return format_response('Parsing error: '+sched_parsed)
    

    # Solve
    res2PL = solve2PL(sched_parsed)
    resConfl = solveConflict(sched_parsed)


    response = ''

    # Format results for 2PL
    if res2PL['sol'] is None:
        #return format_response('<br>'+res2PL['err']+'<br><br>'+res2PL['partial_locks'])
        response = format_response('<br>'+res2PL['err']+'<br><br>')

    else:
        msg = """ <br>
        Solution: {}, <br>
        Is the schedule strict-2PL: <i>{}</i>, <br>
        Is the schedule strong strict-2PL: <i>{}</i> <br>
        """.format(res2PL['sol'], res2PL['strict'], res2PL['strong'])
        response = format_response(msg)

    # Format results for conflict serializability
    msg = 'Is the schedule conflict serializable: '+str(resConfl)
    response = format_response(msg)

    return response


@app.route('/manager/html', methods=['GET'])
def go_away():
    return '<===3'


def format_response(msg):
    return index_cached.replace('<!---->', msg+'<br><!---->')



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
