
from flask import Flask
from flask import request

from check2PL import solve2PL
from utils import print_schedule, parse_schedule

app = Flask(__name__)


index_cached = open('index.html', 'r').read()


@app.route("/", methods=['GET'])
def index():
    schedule = request.args.get('schedule')
    
    if schedule is None:
        return index_cached

    schedule = schedule.replace(' ', '')
    sched_parsed = parse_schedule(schedule)

    if sched_parsed == str:  #error message
        result = sched_parsed

    else:
        result = solve2PL(sched_parsed)

    return index_cached.replace('<!---->', """
<br> <br>



        """)





if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
