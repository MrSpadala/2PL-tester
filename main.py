
import sys

from utils import print_schedule, parse_schedule
from check2PL import solve2PL



if len(sys.argv) < 2:
	print("Usage: "+sys.argv[0]+" \"<schedule>\".\nWhere schedule is in the form r1(x)w3(y)w1(z)...\n")
	schedule = parse_schedule('')
else:
	schedule = parse_schedule(sys.argv[1])


print('SCHEDULE:')
print_schedule(schedule)
print('')


solve2PL(schedule)
