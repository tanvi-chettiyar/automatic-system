class Solution:
    def removeDuplicates(self, nums: List[int]) -> int:
        pre_val = -101
        i = 0
        while i < len(nums):
            if nums[i] == pre_val:
                nums.pop(i)
            else:
                pre_val = nums[i]
                i = i + 1
        k = len(nums)
        return k