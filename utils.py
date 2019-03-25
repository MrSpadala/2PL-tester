
from __future__ import print_function  #comparibility python2

class Operation:
	def __init__(self, type, trans, obj):
		if type==None or trans==None or obj==None:
			raise ValueError
		if type!='READ' and type!='WRITE':
			raise ValueError

		self.type = str(type)  #'READ' or 'WRITE'
		self.transaction = str(trans)
		self.obj = str(obj)


	def __str__(self):
		s = 'r' if self.type=='READ' else 'w'
		return s+self.transaction+'('+self.obj+')'




def parse_schedule(sched):
	"""
	Returns a list of 'Operation' derived from the input schedule as string
	If the input is empty then a test schedule is used
	"""
	if sched == '':
		print('Using test schedule')
		return parse_schedule(TEST_SCHEDULES[0])

	schedule = []

	for i in range(0, len(sched), 5):  #rx(A) has 5 chars
		t, tx, o = None, None, None 

		# get operation type
		if sched[i] == 'r':
			t = 'READ'
		elif sched[i] == 'w':
			t = 'WRITE'
		else:
			raise ValueError('operation types must be \'r\' or \'w\'')

		# get operation transaction
		tx = sched[i+1]

		# check () integrity
		if sched[i+2]!='(' or sched[i+4]!=')':
			raise ValueError('operation transaction and object must be 1 char long')
		
		# get operation object
		o = sched[i+3]

		schedule.append(Operation(t,tx,o))

	return schedule






def print_schedule(sched):
	for trans in sched:
		print(str(trans), end=' ')
	print()


def lock(target, trans, obj):  #pretty string repr of a lock operation
	if target=='UNLOCKED':
		s='u'
	elif target=='SHARED_L':
		s='sl'
	elif target=='XCLUSIVE_L':
		s='xl'
	else:
		raise ValueError
	return s+str(trans)+'('+str(obj)+')'


def print_solution(locks, schedule):
	print('SOLUTION:')
	for locks_i, trans in zip(locks, schedule):
		for l in locks_i:
			print(l, end=' ')
		print(str(trans), end=' ')
	for l in locks[len(schedule)]:  #print final unlocks
		print(l, end=' ')
	print()



# test schedules
TEST_SCHEDULES = [
	'r1(x)r2(z)r1(z)r3(x)r3(y)w1(x)w3(y)r2(y)w4(z)w2(y)',
	'r1(A)r2(A)r2(B)w1(A)w2(D)r3(C)r1(C)w3(B)r4(A)',
	'r1(A)r2(A)r3(B)w1(A)r2(C)r2(B)w2(B)w1(C)',
	'r1(x)w2(x)r3(x)r1(y)r4(z)w2(y)r1(v)w3(v)r4(v)w4(y)w5(y)w5(z)',
	'r1(A)r2(B)r3(C)r1(B)r2(C)r3(D)w1(C)w2(D)w3(E)',
	'r1(A)r2(B)r3(C)r1(B)r2(C)r3(D)w1(A)w2(B)w3(C)',
	'r1(A)r2(B)r3(C)r1(B)r2(C)r3(A)w1(A)w2(B)w3(C)',
	'r1(A)r2(B)r3(C)w1(B)w2(C)w3(A)'
]


