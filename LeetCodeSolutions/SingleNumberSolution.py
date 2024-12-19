class Solution:
    def singleNumber(self, nums: List[int]) -> int:
        for x in nums:
            y = nums.count(x)
            if y == 1:
                return x