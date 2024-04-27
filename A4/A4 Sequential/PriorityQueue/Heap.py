class Heap:
    def __init__(self, elems, comparator=None):
        self.elems = []
        for elm in elems:
            self.insert(elm, comparator)

    def insert(self, elem, comparator):
        # Inserting the last element to the heap
        # Apply heapify method to the elements
        self.elems.append(elem)
        curIdx = len(self.elems) - 1
        while True:
            parent = curIdx // 2  # Computing the parent
            # -1 means parent is greater
            # 0 means parent is equal to curElem
            # 1 means parent is smaller than curElem and needs to be swapped
            if comparator(self.elems[parent], self.elems[curIdx]) == 1:
                # Swap the parent and the current element
                temp = self.elems[curIdx]
                self.elems[curIdx] = self.elems[parent]
                self.elems[parent] = temp
                curIdx = parent
            else:
                break

    def delete(self, comparator):
        if len(self.elems) == 0:
            return None
        # Deletes an element from the heap
        rootElem = self.elems[0]

        # swap last elem and root elem
        self.elems[0] = self.elems[-1]
        self.elems.pop(-1)

        curIdx = 0
        while True:
            child1 = curIdx * 2 + 1
            child2 = curIdx * 2 + 2
            nextIdx = -1
            if child1 <= len(self.elems) - 1 and child2 <= len(self.elems) - 1:

                # Both child exists
                # compare between both child which one is bigger
                ans = comparator(
                    self.elems[child1], self.elems[child2]
                )  # returns -1 if child1 is grater and 0 if equal and 1 if child2
                check = None
                if ans == -1:
                    check = child1
                elif ans == 1:
                    check = child2
                else:
                    break
                # Now we check with the curidx
                ans = comparator(self.elems[curIdx], self.elems[check])
                if ans == 1:
                    # We swap on check is greater
                    # Swap logic
                    temp = self.elems[curIdx]
                    self.elems[curIdx] = self.elems[check]
                    self.elems[check] = temp
                    curIdx = check
                else:
                    break
            elif child1 <= len(self.elems) - 1:
                ans = comparator(self.elems[curIdx], self.elems[child1])
                if ans == 1:
                    temp = self.elems[curIdx]
                    self.elems[curIdx] = self.elems[child1]
                    self.elems[child1] = temp
                    curIdx = child1
                else:
                    break
            else:
                break
        return rootElem

    def printHeap(self):
        print(self.elems)
        return
