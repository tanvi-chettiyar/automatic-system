# Definition for singly-linked list.
class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next
class Solution:
    def sum_list_node(self, l: ListNode, x: int = 1) -> int:
        if not l:
            return 0
        return l.val * x + self.sum_list_node(l.next, x*10)

    def create_list_node(self, var_list) -> ListNode:
        if not var_list:
            return None
        return ListNode(int(var_list[0]), self.create_list_node(var_list[1:]))

    def addTwoNumbers(self, l1: Optional[ListNode], l2: Optional[ListNode]) -> Optional[ListNode]:
        s1 = str(self.sum_list_node(l1))
        s2 = str(self.sum_list_node(l2))
        sum_list = int(s1) + int(s2)
        kkk = self.create_list_node(list(str(sum_list)[::-1]))
        return kkk