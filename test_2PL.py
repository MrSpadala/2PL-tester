
import sys
import itertools
from pprint import pprint

from utils import parse_schedule, print_schedule, lock, get_solution 


if len(sys.argv) < 2:
	print("Usage: "+sys.argv[0]+" \"<schedule>\".\nWhere schedule is in the form r1(x)w3(y)w1(z)...\n")
	schedule = parse_schedule('')
else:
	schedule = parse_schedule(sys.argv[1])


print('SCHEDULE:')
print_schedule(schedule)
print('')


# initialize set of transactions and objects
transactions = set([op.transaction for op in schedule])
objects = set([op.obj for op in schedule])

# states[transaction][object] = state of object from the perspective of transaction (number of states = |transactions|x|objects|)
# a state can be 'START', 'SHARED_L', 'XCLUSIVE_L', 'UNLOCKED'
states = {tx: {o: 'START' for o in objects} for tx in transactions}  

# state of the transaction, if it is in the growing or shrinking phase (#locks)
transaction_state = {tx: 'GROWING' for tx in transactions}

# flags that check if the schedule is 2PL-strict
has_xunlocked = {tx: False for tx in transactions}  #set to true when the transaction unlocks the first exclusive lock
is_strict = True  #set to false if any transaction has xunlocked but performs another operation


# output list storing lock/unlock operations.
# for every transaction i in the schedule, execute locks[i] before schedule[i]
locks = [ [] for i in range(len(schedule)+1)]  #+1 to add unlocks operations at the end






def toState(target, trans, obj, i):
	"""
	Takes care of transitioning 'obj' of 'trans' to state 'target', adding the corresponding
	lock or unlock operation to the solution list and checking whether the change of state is legal and feasible
	"""
	if target!='START' and target!='SHARED_L' and target!='XCLUSIVE_L' and target!='UNLOCKED':
		raise ValueError('Bad target state')

	if states[trans][obj] == target:  #already in target state, do nothing
		return


	if target == 'SHARED_L':
		# Before gathering shared lock on 'obj', I've to unlock other transactions
		# that have 'obj' in exclusive lock
		to_unlock = getTransactionsByState('XCLUSIVE_L', obj)
		for tx in ( to_unlock - set([trans]) ):	unlock(tx, obj, i)	#don't unlock myself

	if target == 'XCLUSIVE_L':
		# Before gathering exlcusive lock on 'obj', I've to unlock other transactions
		# that have 'obj' in shared or exclusive lock
		to_unlock_xl = getTransactionsByState('XCLUSIVE_L', obj)
		to_unlock_sl = getTransactionsByState('SHARED_L', obj)
		to_unlock = set.union(to_unlock_xl, to_unlock_sl)
		for tx in ( to_unlock - set([trans]) ):	unlock(tx, obj, i)	#don't unlock myself


	if transaction_state[trans]=='SHRINKING' and target !='UNLOCKED':
		unfeasible('while processing '+str(schedule[i])+', tansaction '+trans+' has to acquire a lock, '+
					'but it has already performed an unlock operation.', i)

	if target=='UNLOCKED':
		transaction_state[trans] = 'SHRINKING'

	# strictness: check if the schedule unlocks an exclusive lock
	if states[trans][obj]=='XCLUSIVE_L' and target=='UNLOCKED':
		has_xunlocked[trans] = True

	states[trans][obj] = target		#set target state
	locks[i].append(lock(target, trans, obj))   #add the (un)lock operation to the solution







def unlock(trans, obj, i):
	""" Unlock 'obj' for transaction 'trans'.
	Before unlocking it, it must acquire ALL future locks that it
	will need in the future, because once something gets unlocked, 'trans' can no longer acquire
	locks. So it will look in the future operations of 'trans', searching for read
	and write actions on any object: on the matching objects, it will acquire an 
	exclusive lock if 'trans' performs at least one write operation, or, if there are only
	reads, a shared lock. After acquiring those locks finally the lock on 'obj' is released.
	"""
	if states[trans][obj] == 'UNLOCKED' or states[trans][obj] == 'START':
		return

	will_be_read = set()
	will_be_written = set()

	# look in the future transactions of 'trans', starting from transaction at 'i'+1
	#for j in range(i+1, len(schedule)):
	#	operation = schedule[j]
	for operation in schedule[i+1:]:
		if operation.transaction != trans:  #we only need transactions of 'trans'
			continue
		if operation.type == 'READ':
			will_be_read.add(operation.obj)
		elif operation.type == 'WRITE':
			will_be_written.add(operation.obj)
		else:
			raise ValueError

	for obj_to_lock in will_be_written:
		toState('XCLUSIVE_L', trans, obj_to_lock, i)

	for obj_to_lock in (will_be_read - will_be_written):   #if the object will be read and written I have already placed an exlcusive lock on it
		toState('SHARED_L', trans, obj_to_lock, i)

	# Now that I have finally acquired all locks that I will need in the future,
	# 'trans' can unlock 'obj'.
	toState('UNLOCKED', trans, obj, i)







# - - -  utlis  - - - 

def getTransactionsByState(state, obj):
	"""
	returns the set of transactions having object 'obj' in state 'state'
	"""
	trans = filter(lambda tx: states[tx][obj]==state, transactions)
	return set(trans)
		


def put_final_unlocks():
	"""
	Unlocks the all the resources in use at the end of the schedule
	"""
	for trans, obj in itertools.product(transactions, objects):
		state = states[trans][obj]
		if state == 'SHARED_L' or state == 'XCLUSIVE_L':
			toState('UNLOCKED', trans, obj, len(schedule))	


def unfeasible(details=None, i=None):
	s = 'The schedule is not in 2PL'
	if details is None:	print(s+'!')
	else:	print(s+':', details)

	if not i is None: 
		print('\nPartial lock sequence:')
		sol = get_solution(locks, schedule)
		offset = i + sum(map(lambda x: len(x), locks))
		print_schedule(sol[:offset])
	exit(0)









# - - - -  main  - - - -


for i in range(len(schedule)):
	operation = schedule[i]

	obj_state = states[operation.transaction][operation.obj]

	# If a tx has unlocked an exclusive lock and executes another operation, then the whole schedule is not strict
	if has_xunlocked[operation.transaction]:
		is_strict = False


	if operation.type == 'READ':
		
		if obj_state == 'START':  
			toState('SHARED_L', operation.transaction, operation.obj, i)
		
		elif obj_state == 'SHARED_L':  #ok, can continue to read
			pass
		
		elif obj_state == 'XCLUSIVE_L':  #ok, can continue to read
			pass

		elif obj_state == 'UNLOCKED':
			unfeasible('operation '+str(operation)+' needs to lock an unlocked object', i)
		
		else:
			raise ValueError('Bad state')



	elif operation.type == 'WRITE':

		if obj_state == 'START' or obj_state == 'SHARED_L':
			toState('XCLUSIVE_L', operation.transaction, operation.obj, i)

		elif obj_state == 'XCLUSIVE_L':  #ok, can continue to write
			pass

		elif obj_state == 'UNLOCKED':
			unfeasible('operation '+str(operation)+' needs to lock an unlocked object', i)

		else:
			raise ValueError('Bad state')

	else:
		raise ValueError('Bad operation type')

put_final_unlocks()  #unlock active locks
solved_schedule = get_solution(locks, schedule)  #merge locks and the schedule

print('SOLUTION:')
print_schedule(solved_schedule)

print()
print('The schedule IS'+('' if is_strict else ' NOT')+' strict-2PL')





