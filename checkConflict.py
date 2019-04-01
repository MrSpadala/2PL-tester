
def solveConflict(schedule):
	"""Returns true of false whether the schedule is confl. serializable
	"""
    # initialize set of transactions and objects
	transactions = set([op.transaction for op in schedule])

	# initialize precedence graph
	graph = {tx: set() for tx in transactions}

	# populate graph
	for op1, i in zip(schedule, range(len(schedule))):
		for op2 in schedule[i+1:]:
			if op1.obj != op2.obj:
				continue
			if op1.transaction == op2.transaction:
				continue
			if op1.type=='WRITE' or op2.type=='WRITE':
				graph[op1.transaction].add(op2.transaction)

	def DFS(node, visited):
		""" Returns newly visited nodes, None if it has reached
		twice at least one node
		"""
		for children in graph[node]:
			if children in visited:	return None
			visited.add(children)
			dfs_res = DFS(children, visited)
			if not dfs_res is None: 
				visited = visited.union(dfs_res)
			else:
				return None
		return visited

	visited = set()
	while transactions != visited:
		to_visit = transactions - visited
		node = to_visit.pop()
		dfs_res = DFS(node, set([node]))
		if dfs_res is None:
			return False	
		else:
			visited = visited.union(dfs_res)

	return True



if __name__ == '__main__':
	import sys
	from utils import parse_schedule

	schedule_str = 'w3(A)w2(C)r1(A)w1(B)r1(C)r4(A)w4(D)'
	schedule_str = 'w1(x)r2(x)w1(z)r2(z)r3(x)r4(z)w4(z)w2(x)'

	schedule = parse_schedule(schedule_str)

	res = solveConflict(schedule)

	print('Is ser?', res)









