import os
import re
from datetime import datetime
import statistics
N = 10

def TestIfConsensus():
    # 获取结果文件名称
    def GetLogEntriesFileNames():
        prefixfilename = "Outcome.node"
        ret = []
        for i in range(1, N+1):
            ret.append(prefixfilename + str(i))
        return ret
    # 获取文件最后一行数据
    def GetLastLineNumber(filename):
        try:
            with open(filename, 'r') as file:
                last_line = ''
                for line in file:
                    last_line = line.strip()
                if ':' in last_line:
                    number = last_line.split(':')[-1]
                    return number
                else:
                    print("文件格式不正确")
                    return None
                
        except FileNotFoundError:
            print(f"文件 {filename} 不存在")
            return None
        except Exception as e:
            print(f"错误: {str(e)}")
            return None
    filenames = GetLogEntriesFileNames()
    Numbers = []
    for filename in filenames:
        number = GetLastLineNumber(filename)
        Numbers.append(number)
    
    close = len(set(Numbers)) == 1
    if close:
        print("达成共识，结果为")
        print(Numbers[0])
    else:
        print("共识失败，结果为")
        print(Numbers)


def TestVoteTiming():
    class VoteInfo:
        def __init__(self,id:int,term:int,start:str,end:str):
            self.Id = id
            self.Term = term
            self.STime = start
            self.ETime = end
        def VoteTimeGAP(self)->float:
            format_str = "%H:%M:%S.%f"
            stime = datetime.strptime(self.STime, format_str)
            etime = datetime.strptime(self.ETime, format_str)
            # 计算时间差（返回timedelta对象）
            diff = etime - stime
            # 转换为毫秒（总毫秒数）
            total_ms = diff.total_seconds() * 1000
            return total_ms

    def GetLogName():
        prefixfilename = "log.node"
        suffixfilename = ".json"
        ret = []
        for i in range(1,N+1):
            ret.append(prefixfilename+str(i)+suffixfilename)
        return ret
    # print(GetLogName())
    def FindVoteLog(filename):
        Logcomp = re.compile(r'{"level":"(\w+)","msg":"(.+)","time":"(.+)"}')
        ret = []
        try:
            with open(filename, 'r') as file:
                for line in file:
                    line = line.strip()
                    infos = Logcomp.search(line)
                    if infos[1] == "info":
                        if "Start" in infos.group(2):#开始选举log
                            Statemo = re.compile(r"Node (\d)(.*)(\d)")
                            items = Statemo.search(infos.group(2))
                            # print(items.group(1),items.group(3),infos.group(3))
                            ret.append(VoteInfo(items.group(1),items.group(3),infos.group(3),"Unkonw"))
                        if "elected" in infos.group(2):#结束选举log
                            Statemo = re.compile(r"(.*)(\d)(.*)(\d)(.*)")
                            items = Statemo.search(infos.group(2))
                            # print(items.group(2),items.group(4))
                            for i in range(len(ret)):
                                if ret[i].Id == items.group(2) and ret[i].Term == items.group(4):
                                    ret[i].ETime = infos.group(3)
        except FileNotFoundError:
            print(f"文件 {filename} 不存在")
            return None
        except Exception as e:
            print(f"错误: {str(e)}")
            return None
        # if len(ret)>0:
        #     print(ret[0].Id,ret[0].Term,ret[0].STime,ret[0].ETime)
        return ret
    

    filenames = GetLogName()
    VoteInfos = []
    for filename in filenames:
        VoteInfos.extend(FindVoteLog(filename))
    sorted_by_term = sorted(VoteInfos, key=lambda x: x.Term)
    gaps = []
    for item in sorted_by_term:
        print(f"Term {item.Term} Leader is node{item.Id}, timegap:{item.VoteTimeGAP():.2f}")
        gaps.append(item.VoteTimeGAP())
    
    print(f"Avg Vote Timegap: {statistics.mean(gaps)}")

if __name__ == '__main__':
    TestIfConsensus()
    TestVoteTiming()