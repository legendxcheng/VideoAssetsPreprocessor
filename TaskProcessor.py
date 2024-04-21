import pandas as pd
import json
import os
import sys
import re
import ffmpeg
from loguru import logger
import sqlite3
from scenedetect import open_video, SceneManager, split_video_ffmpeg
from moviepy.editor import VideoFileClip
from scenedetect.detectors import AdaptiveDetector
from scenedetect.video_splitter import split_video_ffmpeg
from scenedetect.frame_timecode import FrameTimecode
import subprocess
import shutil
import cv2
import subprocess
import json





def get_video_orientation(video_path):
    # 使用 OpenCV 读取视频宽度和高度
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print("Error opening video file.")
        return None

    width = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
    height = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
    cap.release()

    # 使用 ffprobe 获取旋转信息
    cmd = [
        'ffprobe', '-v', 'error', '-select_streams', 'v:0',
        '-show_entries', 'stream_tags=rotate', '-of', 'json', video_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        rotate = json.loads(result.stdout)['streams'][0]['tags']['rotate']
    except (KeyError, IndexError):
        rotate = "0"

    # 判断视频实际显示方向
    rotate = int(rotate)
    if rotate in (90, 270):  # 视频被旋转了90度或270度
        # 交换宽度和高度
        width, height = height, width
        
    return width, height

def get_video_properties(video_path):
    '''
    获取视频文件的持续时间、宽度和高度。
    '''
    with VideoFileClip(video_path) as video:
        duration = int(video.duration * 1000000)  # 转换为微秒
    width, height = get_video_orientation(video_path)
    return duration, width, height

def split_video_into_scenes(video_path, output_dir , threshold=15.0):
    # Open our video, create a scene manager, and add a detector.
    video = open_video(video_path)
    scene_manager = SceneManager()
    scene_manager.add_detector(
        AdaptiveDetector(adaptive_threshold=2, min_scene_len= 30 ))
    scene_manager.detect_scenes(video, show_progress=True)
    scene_list = scene_manager.get_scene_list()
    if len(scene_list) > 0:
        fps = scene_list[0][0].framerate
        newScneList = []
        for item in scene_list:
            frameNum = item[0].get_frames() + 3
            kk = FrameTimecode(timecode=frameNum, fps=fps)
            newScneList.append((kk, item[1]))
        split_video_ffmpeg(video_path, newScneList, output_dir = output_dir, show_progress=True)
    else:
        
        shutil.copy(video_path, 
                    os.path.join(output_dir, os.path.basename(video_path)[:-4]+"-Scene-001" + video_path[-4:]))
        # split_video_ffmpeg(video_path, scene_list, output_dir = output_dir, show_progress=True)
    pass

def extract_chinese(text):
    # 使用正则表达式匹配所有连续的汉字
    # \u4e00-\u9fff 是汉字在Unicode表中的范围
    pattern = re.compile(r'[\u4e00-\u9fff]+')
    result = pattern.findall(text)
    return result


class TaskProcessor:
    """处理Excel的任务
    """
    def __init__(self):
        self.tasks = []
        with open("config.json", "r", encoding='utf-8') as f:
            self.config = json.load(f)
            
            
            
        
    def preprocess(self):
        df = pd.read_excel("Task.xlsx")
        for index, rowContent in df.iterrows():
            if rowContent['已完成'] == 1 or rowContent['已完成'] == '1':
                continue
            removeAudio = rowContent['删除音频'] == 1 or rowContent['删除音频'] == '1'
            task = {"proj": rowContent['项目'], "tags": rowContent['标签'], 
                        "srcDir": rowContent['原素材目录'], "removeAudio": removeAudio}
            self.tasks.append(task)

    def initDbCursor(self, dbPath):
        """连接SQlite数据库
            表名为 ASSET_TAGS
            字段：id, fileName, tags

        Args:
            dbPath (_type_): db文件的路径
        """
        dirPath = os.path.dirname(dbPath)
        os.makedirs(dirPath, exist_ok=True)
        conn = sqlite3.connect(dbPath)
        curor = conn.cursor()
        curor.execute('''CREATE TABLE IF NOT EXISTS ASSET_TAGS
               (id INTEGER PRIMARY KEY, fileName TEXT, tags TEXT)''')
        conn.commit()
        return conn
    
    def findPatterMp4(self, targetDir, prefix, suffix):
        ret = []
        patternStr = rf"""{prefix}-Scene-\d+\{suffix}"""
        pattern  = re.compile(patternStr)
        for filename in os.listdir(targetDir):
            if pattern.match(filename):
                absPath  = os.path.join(targetDir, filename)
                ret.append(absPath)
        return ret
    
    def process(self):
        for task in self.tasks:
            srcDir = task['srcDir']
            proj = task['proj']
            tags = task['tags']
            removeAudio = task['removeAudio']
            dbPath = os.path.join(self.config['AssetRootDir'], proj, f"metas.db")
            dbConn = self.initDbCursor(dbPath)
            for root, dirs, files in os.walk(srcDir):
                for file in files:
                    # 检查文件扩展名是否为.mp4
                    extraTags = extract_chinese(file)
                    
                    
                    if file.endswith('.mp4') or file.endswith('.MP4') or file.endswith('.mov') or file.endswith('.MOV'):
                        try:
                            absPath  = os.path.join(root, file)
                            vDra, width ,height= get_video_properties(absPath)
                            if width > height:
                                extraTags.append("横屏")
                            else:
                                extraTags.append("竖屏")
                            tmpAbsPath= os.path.join(os.path.dirname(absPath), f"tmp.{file[-3:]}")
                            if removeAudio:
                                # 构建 FFmpeg 命令
                                cmdStr = f"ffmpeg -i \"{absPath}\" -c:v copy -an \"{tmpAbsPath}\""
                                

                                # 执行命令
                                if os.path.exists(tmpAbsPath):
                                    os.remove(tmpAbsPath)
                                subprocess.run(cmdStr, shell=True)
                                os.remove(absPath)
                                os.rename(tmpAbsPath, absPath)
                            outputDir = os.path.join(self.config['AssetRootDir'], proj)
                            split_video_into_scenes(absPath, 
                                output_dir=outputDir)
                            # 分割完视频以后，检查每一个切割后的视频是否存在
                            ntags = tags+""
                            for tagsinFileName in extraTags:
                                if not ntags.endswith(","):
                                    ntags += ","
                                ntags += f" {tagsinFileName}"
                            
                            
                            matchedFiles = self.findPatterMp4(outputDir, 
                                                              os.path.basename(absPath)[:-4], os.path.basename(absPath)[-4:])
                            for filePath in matchedFiles:
                                if os.path.exists(filePath):
                                    dbConn.cursor().execute(f"""INSERT INTO ASSET_TAGS (fileName, tags) VALUES 
                                                        (\"{filePath}\", \"{ntags}\")""")
                                    dbConn.commit()
                                    logger.info(f"""处理好切片{filePath}，并且加入到数据库{dbPath}中""")
                        except Exception as e:
                            logger.error(e)
                            logger.error(f"处理{absPath}出现异常,跳过")
            dbConn.cursor().close()
            dbConn.close()
            task['done'] = True
        # 任务处理完成，要更新Excel
        df = pd.read_excel("Task.xlsx")
        df['已完成'] = 1
        df.to_excel("Task.xlsx", index=False)
        
                        
                        

                        

def main():
    logger.remove(0)
    logger.add(sys.stderr, level="INFO")
    
    tp = TaskProcessor()
    tp.preprocess()
    tp.process()

if __name__ == "__main__":
    main()