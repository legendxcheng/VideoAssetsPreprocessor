import pandas as pd
import json
import os
import sys
import re
import ffmpeg
from loguru import logger
import sqlite3
from scenedetect import open_video, SceneManager, split_video_ffmpeg
from scenedetect.detectors import AdaptiveDetector
from scenedetect.video_splitter import split_video_ffmpeg
from scenedetect.frame_timecode import FrameTimecode
import subprocess

def split_video_into_scenes(video_path, output_dir , threshold=15.0):
    # Open our video, create a scene manager, and add a detector.
    video = open_video(video_path)
    scene_manager = SceneManager()
    scene_manager.add_detector(
        AdaptiveDetector(adaptive_threshold=2, min_scene_len= 30 ))
    scene_manager.detect_scenes(video, show_progress=True)
    scene_list = scene_manager.get_scene_list()
    fps = scene_list[0][0].framerate
    newScneList = []
    for item in scene_list:
        frameNum = item[0].get_frames() + 3
        kk = FrameTimecode(timecode=frameNum, fps=fps)
        newScneList.append((kk, item[1]))
    split_video_ffmpeg(video_path, newScneList, output_dir = output_dir, show_progress=True)
    pass

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
    
    def findPatterMp4(self, targetDir, prefix):
        ret = []
        patternStr = rf"""{prefix}-Scene-\d+\.mp4"""
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
                    if file.endswith('.mp4'):
                        try:
                            absPath  = os.path.join(root, file)
                            tmpAbsPath= os.path.join(os.path.dirname(absPath),"tmp.mp4")
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
                            
                            matchedFiles = self.findPatterMp4(outputDir, 
                                                              os.path.basename(absPath)[:-4])
                            for filePath in matchedFiles:
                                if os.path.exists(filePath):
                                    dbConn.cursor().execute(f"""INSERT INTO ASSET_TAGS (fileName, tags) VALUES 
                                                        (\"{filePath}\", \"{tags}\")""")
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