
from flask import Flask
from flask import request

from check2PL import solve2PL
from utils import parse_schedule

app = Flask(__name__)


index_cached = open('index.html', 'r').read()


@app.route("/2PL", methods=['GET'])
def index():
    schedule = request.args.get('schedule')

    # stupid check
    tmp = map(lambda c: ord(c)<=31 or ord(c)==127, schedule)
    if any(tmp):
        return "<===3"
    
    if schedule is None:
        return index_cached

    schedule = schedule.replace(' ', '')
    if schedule == '':
        return format_response('<br>Empty schedule')

    sched_parsed = parse_schedule(schedule)
    print(sched_parsed)

    if type(sched_parsed) == str:  #error message
        return format_response('Parsing error: '+sched_parsed)
    
    res = solve2PL(sched_parsed)

    if res['sol'] is None:
        #return format_response('<br>'+res['err']+'<br><br>'+res['partial_locks'])
        return format_response('<br>'+res['err']+'<br><br>')

    msg = """ <br>
    Solution: {}, <br>
    Is the schedule strict-2PL: <i>{}</i>, <br>
    Is the schedule strong strict-2PL: <i>{}</i> <br>
    """.format(res['sol'], res['strict'], res['strong'])

    return format_response(msg)


def format_response(msg):
    return index_cached.replace('<!---->', msg)



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
