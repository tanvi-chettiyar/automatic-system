class Solution:
    def maxProfit(self, prices: List[int]) -> int:
        cheap = 10001
        maxprofit = 0
        for x in prices:
            if x < cheap:
                cheap = x
            elif maxprofit < x - cheap:
                maxprofit = x - cheap
        return maxprofit