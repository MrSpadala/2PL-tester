
import itertools
from pprint import pprint

from utils import parse_schedule, format_schedule, lock, get_solution 

def solve2PL(schedule):
	# initialize set of transactions and objects
	transactions = set([op.transaction for op in schedule])
	objects = set([op.obj for op in schedule])

	# states[transaction][object] = state of object from the perspective of transaction (number of states = |transactions|x|objects|)
	# a state can be 'START', 'SHARED_L', 'XCLUSIVE_L', 'UNLOCKED'
	states = {tx: {o: 'START' for o in objects} for tx in transactions}  

	# state of the transaction, if it is in the growing or shrinking phase (#locks)
	transaction_state = {tx: 'GROWING' for tx in transactions}

	# flags that check if the schedule is 2PL-strict
	has_x_unlocked = {tx: False for tx in transactions}  #set to true when the transaction unlocks the first exclusive lock
	is_strict = True  #set to false if any transaction has xunlocked but after performs another operation

	# flags that check if the schedule is strong 2PL-strict
	has_unlocked = {tx: False for tx in transactions}  #set to true when the transaction unlocks the first lock, shared or excl.
	is_strong_strict = True  #set to false if any transaction unlocks any lock but after performs another operation



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
			while True:  #need to update to_unlock every time in case of side effects of unlock
				to_unlock = getTransactionsByState('XCLUSIVE_L', obj)
				to_unlock.discard(trans)   #don't unlock myself
				if len(to_unlock) == 0:	break
				res = unlock(to_unlock.pop(), obj, i)  
				if res:	return res  #if here, unfeasible

		if target == 'XCLUSIVE_L':
			# Before gathering exlcusive lock on 'obj', I've to unlock other transactions
			# that have 'obj' in shared or exclusive lock
			while True:  #need to update to_unlock every time in case of side effects of unlock
				to_unlock_xl = getTransactionsByState('XCLUSIVE_L', obj)
				to_unlock_sl = getTransactionsByState('SHARED_L', obj)
				to_unlock = set.union(to_unlock_xl, to_unlock_sl)
				to_unlock.discard(trans)   #don't unlock myself
				if len(to_unlock) == 0:	break
				res = unlock(to_unlock.pop(), obj, i)
				if res:	return res  #if here, unfeasible



		if transaction_state[trans]=='SHRINKING' and target !='UNLOCKED':
			return unfeasible('while processing '+str(schedule[i])+', tansaction '+trans+' has to acquire a lock, '+
						'but it has already performed an unlock operation.', i)

		if target=='UNLOCKED':
			transaction_state[trans] = 'SHRINKING'

		# strictness: check if the schedule unlocks an exclusive lock
		if states[trans][obj]=='XCLUSIVE_L' and target=='UNLOCKED':
			has_x_unlocked[trans] = True

		# strong strictness: check if the schedule unlocks any lock
		if (states[trans][obj]=='XCLUSIVE_L' or states[trans][obj]=='SHARED_L') and target=='UNLOCKED':
			has_unlocked[trans] = True

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
			res = toState('XCLUSIVE_L', trans, obj_to_lock, i)
			if res:	return res

		for obj_to_lock in (will_be_read - will_be_written):   #if the object will be read and written I have already placed an exlcusive lock on it
			res = toState('SHARED_L', trans, obj_to_lock, i)
			if res:	return res

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


	def unfeasible(details, i):
		s = 'The schedule is not in 2PL'
		if details is None:	s += '!'
		else:	s += ': '+details

		s1 = '\nPartial lock sequence: '
		if not i is None: 
			sol = get_solution(locks, schedule)
			offset = i + sum(map(lambda x: len(x), locks))
			s1 += format_schedule(sol[:offset])
		
		return {'sol': None, 'partial_locks': s1, 'err': s}









	# - - - -  main  - - - -


	for i in range(len(schedule)):
		operation = schedule[i]

		obj_state = states[operation.transaction][operation.obj]

		# If a tx has unlocked an exclusive lock and executes another operation, then the whole schedule is not strict
		if has_x_unlocked[operation.transaction]:
			is_strict = False

		# If a tx has unlocked any lock and executes another operation, then the whole schedule is not strong strict
		if has_unlocked[operation.transaction]:
			is_strong_strict = False


		if operation.type == 'READ':
			
			if obj_state == 'START':  
				res = toState('SHARED_L', operation.transaction, operation.obj, i)
				if res:	return res
			
			elif obj_state == 'SHARED_L':  #ok, can continue to read
				pass
			
			elif obj_state == 'XCLUSIVE_L':  #ok, can continue to read
				pass

			elif obj_state == 'UNLOCKED':
				return unfeasible('operation '+str(operation)+' needs to lock an unlocked object', i)
			
			else:
				raise ValueError('Bad state')



		elif operation.type == 'WRITE':

			if obj_state == 'START' or obj_state == 'SHARED_L':
				res = toState('XCLUSIVE_L', operation.transaction, operation.obj, i)
				if res:	return res

			elif obj_state == 'XCLUSIVE_L':  #ok, can continue to write
				pass

			elif obj_state == 'UNLOCKED':
				return unfeasible('operation '+str(operation)+' needs to lock an unlocked object', i)

			else:
				raise ValueError('Bad state')

		else:
			raise ValueError('Bad operation type')

	put_final_unlocks()  #unlock active locks
	solved_schedule = get_solution(locks, schedule)  #merge locks and the schedule

	#print('SOLUTION:')
	solved_schedule_str = format_schedule(solved_schedule)

	#print()
	#print('The schedule IS'+('' if is_strict else ' NOT')+' strict-2PL')

	#print()
	#print('The schedule IS'+('' if is_strong_strict else ' NOT')+' strong strict-2PL')

	return {
		'sol': solved_schedule_str,
		'strict': str(is_strict),
		'strong': str(is_strong_strict)
	}




