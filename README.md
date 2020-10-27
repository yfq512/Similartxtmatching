## 功能
在本地文本数据集中匹配与目标文本相似度高于阈值的文本，文本相似度算法参考[https://github.com/yongzhuo/Macropodus]，基于Flask实现web服务，本地图像数据集支持热更新

## 环境
不需要特殊环境

## 运行
python server.py

## 接口API
http://wiki.ccwb.cn/web/#/81?page_id=2405

## 其他
* 首次运行需创建文件夹：orgtxts（用于存放本地文本文件），uporgtxts（用于暂存文本数据热更新时需要的文件）
* 当使用“热更新”功能后，需调用示例post.py请求的接口后热更新才会完全生效
* post.py # 文本相似度匹配调用示例
* post2.py # 热更新-添加文本调用示例
* post3.py # 热更新-删除文本调用示例
* winnowing.py # 文本相似度计算时用到的函数
