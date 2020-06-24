
from collections import defaultdict
from utils import parse_schedule, _sched_malformed_err


from os.path import isfile
DEBUG = isfile('.DEBUG')
def debug(*args):
	msg = ''
	for arg in args:
		msg += str(arg)+' '
	if DEBUG:	print(msg)



# WORK IN PROGRESS


def solveTimestamps(schedule):
	"""
	Returns true of false whether the schedule is serializable through timestamps
	"""

	# timestamps information for each object
	# indices of the dummy_entry array where the timestamp information is stored
	RTS, WTS, WTS_C, CB = 'RTS', 'WTS', 'WTS_C', 'CB'
	dummy_entry = {RTS:-1, WTS:-1 , WTS_C:-1 , CB:1} #dummy entry, all entries initialized like this
	timestamps_data = {op.obj: dummy_entry.copy() for op in schedule}


	# save objects written by each transaction
	written_obj = defaultdict(set)   #dict[transaction] = written objects by transaction

	# Set of waiting transactions
	#if 'T' is a waiting transaction, then waiting_tx['T'] is the index of the operation of 'T' in the schedule where 'T' is blocked
	waiting_tx = dict()  

	# final solution. Its entries are lists
	solution = list()
	solution_entry = list()


	# - - - - utils - - -

	def commit(tx):
		"""Performs the commit of transaction 'tx'
		"""
		solution_entry.append('commit')
		debug('committing', tx)
		for obj in written_obj[tx]:
			data = timestamps_data[obj]
			set_timestamp_data(obj, WTS_C, data[WTS])
			set_timestamp_data(obj, CB, True)

	def rollback(tx):
		"""Performs the rollback of transaction 'tx'
		"""
		solution_entry.append('rollback')
		debug('rollbacking', tx)
		for obj in written_obj[tx]:
			data = timestamps_data[obj]
			set_timestamp_data(obj, WTS, data[WTS_C])
			set_timestamp_data(obj, CB, True)

	def execute(op):
		"""Execute operation op. Write in the solution its execution and,
		whether it is the last, the commit of its transaction
		"""
		if op.type == 'WRITE':
			written_obj[op.transaction].add(op.obj)
		if not operation.tx_continues:
			commit(operation.transaction)

	def set_timestamp_data(obj, key, value):
		if key!=CB and key!=WTS and key!=WTS_C and key!=RTS:
			raise ValueError
		timestamps_data[obj][key] = value
		solution_entry.append(f'{key}({str(obj)})={value}')


	def TS(tx):
		"""Returns the timestamp of a transaction.
		we ASSUME that timestamp of transaction 'i' is 'i'
		"""
		return int(tx)

	# sanity check on the timestamps
	try:
		err = None
		all_timestamps = [TS(op.transaction) for op in schedule]
		negative_ts = filter(lambda x: x<0, all_timestamps)
		if len(list(negative_ts)) > 0:
			err = _sched_malformed_err('Transactions (their timestamps) must be non negative')
	except ValueError:
		err = _sched_malformed_err('Transactions (their timestamps) must be integers')
	finally:
		if err:	return {'err': err}




	# - - - - main - - -

	i = 0
	while True:

		debug('\nNEW STEP')

		# Fetch new operation

		# First try to fetch the next operation if a transaction has been unlocked
		locking_ops_index = sorted(waiting_tx.values())
		for i_lock in locking_ops_index:
			obj = schedule[i_lock].obj
			if timestamps_data[obj][CB] == True:   #if the commit bit of an object, waited by some transaction, becomes true then fetch the operation
				operation = schedule[i_lock]
				waiting_tx.pop(operation.transaction)
				solution_entry.append(f'resume {str(operation.transaction)}')
				break

		else:  # no waiting operation, go on with the schedule
			if i >= len(schedule):
				# If here, there are no waiting operation and the schedule is empty, can return solution
				#return solution
				return {'err': None, 'sol': format_solution(solution), 'waiting_tx': waiting_tx}

			operation = schedule[i]
			debug('Picked operation from schedule', operation)
			# Check if transaction of the operation is in wait, if so skip it
			tx = operation.transaction
			if tx in waiting_tx:
				debug('Transaction is in waiting', operation)
				i += 1
				continue

		# Now we have fetched the next operation, execute it
		debug('executing operation', operation)

		# New entry that will be populated and pushed into the solution
		solution_entry.append(str(operation))

		transaction = operation.transaction
		obj = operation.obj
		ts_obj = timestamps_data[obj]

		if operation.type == 'READ':
			if TS(transaction) >= ts_obj[WTS]:
				if ts_obj[CB] or TS(transaction) == ts_obj[WTS]:
					set_timestamp_data(obj, RTS, max(TS(transaction), ts_obj[RTS]))
					execute(operation)
				else:
					debug('put operation in wait', operation)
					waiting_tx[transaction] = i
					solution_entry.append('Wait')
			else:
				rollback(transaction)

		elif operation.type == 'WRITE':
			if TS(transaction) >= ts_obj[RTS] and TS(transaction) >= ts_obj[WTS]:
				if ts_obj[CB]:
					set_timestamp_data(obj, WTS, TS(transaction))
					set_timestamp_data(obj, CB, False)
					execute(operation)
				else:
					debug('put operation in wait', operation)
					waiting_tx[transaction] = i
					solution_entry.append('Wait')
			else:
				if TS(transaction) >= ts_obj[RTS] and TS(transaction) < ts_obj[WTS]:
					if ts_obj[CB]:
						solution_entry.append('Ignored (Thomas rule)')
					else:
						debug('put operation in wait', operation)
						waiting_tx[transaction] = i
						solution_entry.append('Wait')
				else:
					rollback(transaction)
			
		else:
			raise ValueError('Bad operation type')

		i += 1  #update schedule index
		# append data to solution
		solution.append(solution_entry.copy())
		solution_entry.clear()



def format_solution(sol):
	#s = '<table><tr><th><b>Operation</b></th><th><b>Action</b></th></tr>'
	s = '<table>'
	for action in sol:
		s += f'<tr><td>{action[0]}</td><td>'  #action[0] is the operation
		for op in action[1:]:
			s += op + ' '
		s += '</td></tr>'
	return s + '</table>'
	



if __name__ == '__main__':
	schedule = parse_schedule('w1(x)r2(x)w1(y)')
	#schedule = parse_schedule('')
	solution = solveTimestamps(schedule)
	from pprint import pprint
	pprint(solution)


