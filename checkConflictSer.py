
def solveConflictSer(schedule):
    # initialize set of transactions and objects
	transactions = set([op.transaction for op in schedule])

	# initialize precedence graph
	graph = {tx: list() for tx in transactions}

	# populate graph
	for op1, i in zip(schedule, range(len(schedule))):
		for op2 in schedule[i+1:]:
			if op1.obj != op2.obj:
				continue
			if op1.transaction == op2.transaction:
				continue
			if op1.type=='WRITE' or op2.type=='WRITE':
				graph[op1.transaction].append(op2.transaction)

	def DFS(node, visited):
		for children in graph[node]:
			if children in visited:	return None
			visited.add(children)
			DFS(children, visited)
		return visited

	visited = set()
	while transactions == visited:
		to_visit = transactions - visited
		node = to_visit.pop()
		res = DFS(node, visited)
		if res is None:	print('NO SER!')
		else:	visited.add(res)









