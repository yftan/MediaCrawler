# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：  
# 1. 不得用于任何商业用途。  
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。  
# 3. 不得进行大规模爬取或对平台造成运营干扰。  
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。   
# 5. 不得用于任何非法或不当的用途。
#   
# 详细许可条款请参阅项目根目录下的LICENSE文件。  
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。  


import asyncio
import copy
import json
import urllib.parse
from typing import Any, Callable, Dict, Optional

import requests
from playwright.async_api import BrowserContext

from base.base_crawler import AbstractApiClient
from tools import utils
from var import request_keyword_var

from .exception import *
from .field import *
from .help import *


class DOUYINClient(AbstractApiClient):
    def __init__(
            self,
            timeout=30,
            proxies=None,
            *,
            headers: Dict,
            playwright_page: Optional[Page],
            cookie_dict: Dict
    ):
        self.proxies = proxies
        self.timeout = timeout
        self.headers = headers
        self._host = "https://www.douyin.com"
        self.playwright_page = playwright_page
        self.cookie_dict = cookie_dict

    async def __process_req_params(
            self, uri: str, params: Optional[Dict] = None, headers: Optional[Dict] = None,
            request_method="GET"
    ):
        """处理抖音API请求参数
        参数:
            uri: 请求的接口路径
            params: 请求参数字典
            headers: 请求头字典
            request_method: 请求方法(GET/POST)
        
        主要功能:
        1. 添加通用请求参数
        2. 获取浏览器本地存储信息
        3. 生成防爬签名(a_bogus)
        """
        # 如果没有参数则直接返回
        if not params:
            return
        
        # 使用传入的headers或默认headers
        headers = headers or self.headers
        
        # 从浏览器获取本地存储信息(用于msToken)
        local_storage: Dict = await self.playwright_page.evaluate("() => window.localStorage")  # type: ignore
        
        # 构建通用请求参数
        common_params = {
            # 设备和平台信息
            "device_platform": "webapp",
            "platform": "PC",
            "pc_client_type": "1",
            
            # APP相关参数
            "aid": "6383",  # 抖音Web应用ID
            "channel": "channel_pc_web",
            "version_code": "190600",
            "version_name": "19.6.0",
            "update_version_code": "170400",
            
            # 浏览器环境参数
            "cookie_enabled": "true",
            "browser_language": "zh-CN",
            "browser_platform": "MacIntel",
            "browser_name": "Chrome",
            "browser_version": "125.0.0.0",
            "browser_online": "true",
            
            # 系统环境参数
            "engine_name": "Blink",
            "engine_version": "109.0",
            "os_name": "Mac OS", 
            "os_version": "10.15.7",
            
            # 硬件参数
            "cpu_core_num": "8",
            "device_memory": "8",
            "screen_width": "2560",
            "screen_height": "1440",
            
            # 网络参数
            "effective_type": "4g",
            "round_trip_time": "50",
            
            # 用户标识参数
            "webid": get_web_id(),  # 生成随机webid
            "msToken": local_storage.get("xmst"),  # 从本地存储获取msToken
        }
        
        # 将通用参数更新到请求参数中
        params.update(common_params)
        
        # 将参数转换为URL查询字符串
        query_string = urllib.parse.urlencode(params)

        # 20240927 a-bogus更新（JS版本）
        post_data = {}
        if request_method == "POST":
            post_data = params
        
        # 生成防爬签名(a_bogus)并添加到参数中
        a_bogus = await get_a_bogus(uri, query_string, post_data, headers["User-Agent"], self.playwright_page)
        params["a_bogus"] = a_bogus

    async def request(self, method, url, **kwargs):
        """通用HTTP请求处理方法
        参数:
            method: 请求方法(GET/POST)
            url: 请求URL
            **kwargs: 其他请求参数(headers, params等)
        返回:
            Dict: 响应JSON数据
        异常:
            DataFetchError: 数据获取失败时抛出
        """
        response = None
        # 发送GET或POST请求
        if method == "GET":
            response = requests.request(method, url, **kwargs)
        elif method == "POST":
            response = requests.request(method, url, **kwargs)
        
        try:
            # 检查响应内容是否表明账号被封禁
            if response.text == "" or response.text == "blocked":
                utils.logger.error(f"request params incrr, response.text: {response.text}")
                raise Exception("account blocked")
            # 返回JSON格式的响应数据
            return response.json()
        except Exception as e:
            # 包装异常信息并抛出
            raise DataFetchError(f"{e}, {response.text}")

    async def get(self, uri: str, params: Optional[Dict] = None, headers: Optional[Dict] = None):
        """发送GET请求
        参数:
            uri: API路径
            params: 请求参数
            headers: 请求头
        返回:
            Dict: 响应数据
        流程:
        1. 处理请求参数
        2. 设置请求头
        3. 发送请求
        """
        # 处理请求参数(添加通用参数和签名)
        await self.__process_req_params(uri, params, headers)
        # 使用默认headers或传入的headers
        headers = headers or self.headers
        # 发送GET请求
        return await self.request(method="GET", url=f"{self._host}{uri}", params=params, headers=headers)

    async def post(self, uri: str, data: dict, headers: Optional[Dict] = None):
        """发送POST请求
        参数:
            uri: API路径
            data: POST数据
            headers: 请求头
        """
        # 处理请求参数
        await self.__process_req_params(uri, data, headers)
        headers = headers or self.headers
        # 发送POST请求
        return await self.request(method="POST", url=f"{self._host}{uri}", data=data, headers=headers)

    async def pong(self, browser_context: BrowserContext) -> bool:
        """检查登录状态
        参数:
            browser_context: 浏览器上下文
        返回:
            bool: 是否已登录
        检查方式:
        1. 检查localStorage中的登录标志
        2. 检查cookie中的登录状态
        """
        # 检查localStorage中的登录标志
        local_storage = await self.playwright_page.evaluate("() => window.localStorage")
        if local_storage.get("HasUserLogin", "") == "1":
            return True

        # 检查cookie中的登录状态
        _, cookie_dict = utils.convert_cookies(await browser_context.cookies())
        return cookie_dict.get("LOGIN_STATUS") == "1"

    async def update_cookies(self, browser_context: BrowserContext):
        """更新cookies信息
        参数:
            browser_context: 浏览器上下文
        功能:
        1. 获取并转换cookies
        2. 更新headers和cookie字典

        在以下情况需要调用:
        1. 初始登录后
        2. 会话刷新时
        3. 检测到登录状态变化时
        """
        # 获取并转换cookies
        cookie_str, cookie_dict = utils.convert_cookies(await browser_context.cookies())
        # 更新headers中的Cookie和cookie字典
        self.headers["Cookie"] = cookie_str
        self.cookie_dict = cookie_dict

    async def search_info_by_keyword(
            self,
            keyword: str,
            offset: int = 0,
            search_channel: SearchChannelType = SearchChannelType.GENERAL,
            sort_type: SearchSortType = SearchSortType.GENERAL,
            publish_time: PublishTimeType = PublishTimeType.UNLIMITED,
            search_id: str = ""
    ):
        """抖音关键词搜索API
        参数:
            keyword: 搜索关键词
            offset: 分页偏移量
            search_channel: 搜索频道类型(综合/视频/用户等)
            sort_type: 排序方式(综合排序/最新发布等)
            publish_time: 发布时间筛选(不限/一天内/一周内等)
            search_id: 搜索会话ID
        返回:
            Dict: 搜索结果数据
        """
        # 构建基础查询参数
        query_params = {
            'search_channel': search_channel.value,  # 搜索频道
            'enable_history': '1',                   # 启用历史记录
            'keyword': keyword,                      # 搜索关键词
            'search_source': 'tab_search',           # 搜索来源
            'query_correct_type': '1',               # 查询纠正类型
            'is_filter_search': '0',                 # 是否过滤搜索
            'from_group_id': '7378810571505847586', # 来源组ID
            'offset': offset,                        # 分页偏移
            'count': '15',                          # 每页数量
            'need_filter_settings': '1',             # 需要过滤设置
            'list_type': 'multi',                   # 列表类型
            'search_id': search_id,                 # 搜索会话ID
        }

        # 如果指定了排序方式或发布时间，添加过滤条件
        if sort_type.value != SearchSortType.GENERAL.value or publish_time.value != PublishTimeType.UNLIMITED.value:
            query_params["filter_selected"] = json.dumps({
                "sort_type": str(sort_type.value),      # 排序方式
                "publish_time": str(publish_time.value)  # 发布时间
            })
            query_params["is_filter_search"] = 1        # 启用过滤搜索
            query_params["search_source"] = "tab_search" # 设置搜索来源

        # 构建Referer URL（模拟从搜索页面发起请求）
        referer_url = f"https://www.douyin.com/search/{keyword}?aid=f594bbd9-a0e2-4651-9319-ebe3cb6298c1&type=general"
        # 复制headers并更新Referer
        headers = copy.copy(self.headers)
        headers["Referer"] = urllib.parse.quote(referer_url, safe=':/')
        
        # 发送搜索请求
        return await self.get("/aweme/v1/web/general/search/single/", query_params, headers=headers)

    async def get_video_by_id(self, aweme_id: str) -> Any:
        """获取抖音视频详情API
        参数:
            aweme_id: 视频ID
        返回:
            Dict: 视频详细信息
        说明:
            1. 删除Origin头，避免跨域问题
            2. 只返回视频详情部分数据
        """
        # 构建请求参数
        params = {
            "aweme_id": aweme_id
        }
        
        # 复制headers并删除Origin头
        headers = copy.copy(self.headers)
        del headers["Origin"]
        
        # 发送请求并只返回视频详情部分
        res = await self.get("/aweme/v1/web/aweme/detail/", params, headers)
        return res.get("aweme_detail", {})

    async def get_aweme_comments(self, aweme_id: str, cursor: int = 0):
        """获取视频评论列表
        参数:
            aweme_id: 视频ID
            cursor: 分页游标，默认从0开始
        返回:
            Dict: 包含评论列表的响应数据
        
        说明:
        1. 每次请求返回20条评论
        2. 通过cursor实现分页
        3. 需要设置正确的Referer以模拟来自搜索页面的请求
        """
        # 评论列表API路径
        uri = "/aweme/v1/web/comment/list/"
        
        # 构建请求参数
        params = {
            "aweme_id": aweme_id,    # 视频ID
            "cursor": cursor,         # 分页游标
            "count": 20,             # 每页评论数量
            "item_type": 0           # 评论类型
        }
        
        # 获取当前搜索关键词（用于构建Referer）
        keywords = request_keyword_var.get()
        # 构建Referer URL，模拟从搜索结果页访问
        referer_url = "https://www.douyin.com/search/" + keywords + '?aid=3a3cec5a-9e27-4040-b6aa-ef548c2c1138&publish_time=0&sort_type=0&source=search_history&type=general'
        
        # 复制headers并设置Referer
        headers = copy.copy(self.headers)
        headers["Referer"] = urllib.parse.quote(referer_url, safe=':/')
        
        # 发送请求获取评论数据
        return await self.get(uri, params)

    async def get_sub_comments(self, comment_id: str, cursor: int = 0):
        """获取评论的回复列表（子评论）
        参数:
            comment_id: 父评论ID
            cursor: 分页游标，默认从0开始
        返回:
            Dict: 包含子评论列表的响应数据
        
        说明:
        1. 每次请求返回20条子评论
        2. 通过cursor实现分页
        3. 需要与主评论请求保持一致的Referer设置
        """
        # 子评论API路径
        uri = "/aweme/v1/web/comment/list/reply/"
        
        # 构建请求参数
        params = {
            'comment_id': comment_id,  # 父评论ID
            "cursor": cursor,          # 分页游标
            "count": 20,              # 每页评论数量
            "item_type": 0,           # 评论类型
        }
        
        # 获取当前搜索关键词（用于构建Referer）
        keywords = request_keyword_var.get()
        # 构建Referer URL，保持与主评论请求一致
        referer_url = "https://www.douyin.com/search/" + keywords + '?aid=3a3cec5a-9e27-4040-b6aa-ef548c2c1138&publish_time=0&sort_type=0&source=search_history&type=general'
        
        # 复制headers并设置Referer
        headers = copy.copy(self.headers)
        headers["Referer"] = urllib.parse.quote(referer_url, safe=':/')
        
        # 发送请求获取子评论数据
        return await self.get(uri, params)

    async def get_aweme_all_comments(
            self,
            aweme_id: str,
            crawl_interval: float = 1.0,
            is_fetch_sub_comments=False,
            callback: Optional[Callable] = None,
            max_count: int = 10,
    ):
        """获取视频的所有评论（包括子评论）
        参数:
            aweme_id: 视频ID
            crawl_interval: 爬取间隔时间(秒)
            is_fetch_sub_comments: 是否获取子评论
            callback: 评论处理回调函数
            max_count: 一次帖子爬取的最大评论数量
        返回:
            List: 评论列表
        """
        result = []  # 存储所有评论
        comments_has_more = 1  # 是否还有更多评论
        comments_cursor = 0    # 评论分页游标
        
        # 获取主评论，直到没有更多或达到数量上限
        while comments_has_more and len(result) < max_count:
            # 获取一页主评论
            comments_res = await self.get_aweme_comments(aweme_id, comments_cursor)
            comments_has_more = comments_res.get("has_more", 0)
            comments_cursor = comments_res.get("cursor", 0)
            comments = comments_res.get("comments", [])
            
            if not comments:
                continue
            
            # 如果加入新评论会超过上限，则截取部分
            if len(result) + len(comments) > max_count:
                comments = comments[:max_count - len(result)]
            
            # 添加评论到结果列表
            result.extend(comments)
            
            # 执行回调函数（如果有）
            if callback:
                await callback(aweme_id, comments)

            # 延时等待
            await asyncio.sleep(crawl_interval)
            
            # 如果不需要获取子评论，继续下一轮
            if not is_fetch_sub_comments:
                continue
            
            # 获取每条主评论的子评论
            for comment in comments:
                reply_comment_total = comment.get("reply_comment_total")

                # 如果有子评论
                if reply_comment_total > 0:
                    comment_id = comment.get("cid")
                    sub_comments_has_more = 1
                    sub_comments_cursor = 0

                    # 获取所有子评论
                    while sub_comments_has_more:
                        sub_comments_res = await self.get_sub_comments(comment_id, sub_comments_cursor)
                        sub_comments_has_more = sub_comments_res.get("has_more", 0)
                        sub_comments_cursor = sub_comments_res.get("cursor", 0)
                        sub_comments = sub_comments_res.get("comments", [])

                        if not sub_comments:
                            continue
                        # 添加子评论到结果列表
                        result.extend(sub_comments)
                        # 执行回调函数（如果有）
                        if callback:
                            await callback(aweme_id, sub_comments)
                        # 延时等待
                        await asyncio.sleep(crawl_interval)
        return result

    async def get_user_info(self, sec_user_id: str):
        """获取用户信息
        参数:
            sec_user_id: 用户ID
        """
        uri = "/aweme/v1/web/user/profile/other/"
        params = {
            "sec_user_id": sec_user_id,
            "publish_video_strategy_type": 2,  # 发布视频策略类型
            "personal_center_strategy": 1,      # 个人中心策略
        }
        return await self.get(uri, params)

    async def get_user_aweme_posts(self, sec_user_id: str, max_cursor: str = "") -> Dict:
        """获取用户发布的视频列表（单页）
        参数:
            sec_user_id: 用户ID
            max_cursor: 分页标记
        """
        uri = "/aweme/v1/web/aweme/post/"
        params = {
            "sec_user_id": sec_user_id,
            "count": 18,                # 每页数量
            "max_cursor": max_cursor,   # 分页标记
            "locate_query": "false",
            "publish_video_strategy_type": 2,
            'verifyFp': 'verify_lx901cuk_K7kaK4dK_bn2E_4dgk_BxAA_E0XS1VtUi130',
            'fp': 'verify_lx901cuk_K7kaK4dK_bn2E_4dgk_BxAA_E0XS1VtUi130'
        }
        return await self.get(uri, params)

    async def get_all_user_aweme_posts(self, sec_user_id: str, callback: Optional[Callable] = None):
        """获取用户的所有视频
        参数:
            sec_user_id: 用户ID
            callback: 视频处理回调函数
        """
        posts_has_more = 1  # 是否还有更多视频
        max_cursor = ""     # 分页标记
        result = []         # 存储所有视频
        
        # 循环获取所有视频
        while posts_has_more == 1:
            # 获取一页视频
            aweme_post_res = await self.get_user_aweme_posts(sec_user_id, max_cursor)
            posts_has_more = aweme_post_res.get("has_more", 0)
            max_cursor = aweme_post_res.get("max_cursor")
            aweme_list = aweme_post_res.get("aweme_list") if aweme_post_res.get("aweme_list") else []
            
            # 记录日志
            utils.logger.info(
                f"[DOUYINClient.get_all_user_aweme_posts] got sec_user_id:{sec_user_id} video len : {len(aweme_list)}")
            
            # 执行回调函数（如果有）
            if callback:
                await callback(aweme_list)
            
            # 添加视频到结果列表
            result.extend(aweme_list)
        return result
