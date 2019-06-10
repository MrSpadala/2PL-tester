
from flask import Flask
from flask import request

from check2PL import solve2PL
from checkConflict import solveConflict
from checkTimestamps import solveTimestamps
from utils import parse_schedule

app = Flask(__name__)


index_cached = open('index.html', 'r').read()


@app.route("/2PL", methods=['GET'])
def index():
    # get and check args
    schedule    = request.args.get('schedule')
    use_xl_only = request.args.get('use_xl_only')

    response = index_cached

    if schedule is None:
        return response    

    schedule = schedule.replace(' ', '')
    if schedule == '':
        return format_response('Empty schedule', response)

    sched_parsed = parse_schedule(schedule)
    print(sched_parsed)

    if type(sched_parsed) == str:  #parsing error message
        return format_response('Parsing error: '+sched_parsed, response)
    

    # Solve
    res2PL = solve2PL(sched_parsed, use_xl_only)
    resConfl = solveConflict(sched_parsed)
    resTS = solveTimestamps(sched_parsed)


    # Format results for conflict serializability
    msg = '<b><i>Conflict serializability</i></b><br>'
    #msg += 'Is the schedule conflict serializable: <i>'+str(resConfl)+'</i>'
    # TODO fix problematic schedule r3(z) r1(z)w2(y)w4(x)w3(z)w3(y) r1(x)w2(x)
    msg += 'work in progress'
    response = format_response(msg, response)

    # Format results for 2PL
    msg = '<b><i>Two phase lock protocol</i></b><br>'
    if res2PL['sol'] is None:
        #return format_response('<br>'+res2PL['err']+'<br><br>'+res2PL['partial_locks'])
        msg += res2PL['err']
        response = format_response(msg, response)
    else:
        msg += """
        Solution: {}, <br>
        Is the schedule strict-2PL: <i>{}</i>, <br>
        Is the schedule strong strict-2PL: <i>{}</i>
        """.format(res2PL['sol'], res2PL['strict'], res2PL['strong'])
        response = format_response(msg, response)

    # Format results for timestamps
    msg = '<b><i>Timestamps (DRAFT)</i></b><br>'
    if resTS['err'] is None:
        msg += 'List of executed operations: '+str(resTS['sol'])+'<br>'
        msg += 'List of waiting transactions at the end of schedule: '+str(resTS['waiting_tx'])+'<br>'
        response = format_response(msg, response)
    else:
        msg += resTS['err']+'<br>'
        response = format_response(msg, response)

    return response



def format_response(msg, res):    
    return res.replace('<!---->', '<br>'+msg+'<br><!---->')



if __name__ == "__main__":
    from os.path import isfile
    debug = isfile('.DEBUG')
    host = "localhost" if debug else "0.0.0.0"
    app.run(host=host, port=8080, debug=debug)
