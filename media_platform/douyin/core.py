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
import os
import random
from asyncio import Task
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright)

import config
from base.base_crawler import AbstractCrawler
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import douyin as douyin_store
from tools import utils
from var import crawler_type_var, source_keyword_var

from .client import DOUYINClient
from .exception import DataFetchError
from .field import PublishTimeType
from .login import DouYinLogin


class DouYinCrawler(AbstractCrawler):
    context_page: Page
    dy_client: DOUYINClient
    browser_context: BrowserContext

    def __init__(self) -> None:
        self.index_url = "https://www.douyin.com"

    async def add_request_delay(self, name: str = "请求") -> float:
        """添加请求延迟，防止被识别为爬虫
        
        参数:
            name: 操作名称，用于日志显示
            
        返回:
            float: 实际延迟的秒数
        """
        # 获取配置的延迟时间，默认为10秒
        delay = config.REQUEST_DELAY if hasattr(config, "REQUEST_DELAY") else 10
        # 增加0-2秒的随机延迟
        random_delay = delay + random.uniform(0, 2)
        utils.logger.info(f"[DouYinCrawler.add_request_delay] {name}前等待 {random_delay:.2f}秒...")
        await asyncio.sleep(random_delay)
        return random_delay

    async def start(self) -> None:
        # 初始化代理变量
        playwright_proxy_format, httpx_proxy_format = None, None
        
        # 如果启用了IP代理功能
        if config.ENABLE_IP_PROXY:
            # 1. 创建IP代理池,参数说明:
            # - config.IP_PROXY_POOL_COUNT: 代理池大小
            # - enable_validate_ip=True: 启用IP可用性验证
            ip_proxy_pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, enable_validate_ip=True)
            
            # 2. 从代理池获取一个可用代理
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
            
            # 3. 格式化代理信息,转换成不同客户端需要的格式
            # - playwright_proxy_format: 用于浏览器自动化
            # - httpx_proxy_format: 用于HTTP请求
            playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(ip_proxy_info)

        async with async_playwright() as playwright:
            # 1. 初始化浏览器
            chromium = playwright.chromium
            # 启动浏览器实例，可配置代理、UA、是否显示界面等
            self.browser_context = await self.launch_browser(
                chromium,
                None,  # proxy
                user_agent=None,
                headless=config.HEADLESS  # True则不显示浏览器界面
            )
            
            # 2. 反爬虫配置
            # 注入stealth.min.js脚本，用于屏蔽浏览器自动化特征
            # 这个脚本会修改一些浏览器特征，使网站难以识别是爬虫
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            
            # 3. 创建新标签页并访问首页
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url)  # 访问抖音首页

            # 4. 创建抖音API客户端
            self.dy_client = await self.create_douyin_client(httpx_proxy_format)
            
            # 5. 登录流程
            # 首先测试当前会话是否有效
            if not await self.dy_client.pong(browser_context=self.browser_context):
                # 会话无效，需要登录
                login_obj = DouYinLogin(
                    login_type=config.LOGIN_TYPE,  # 登录方式：qrcode/cookie
                    login_phone="",  # 手机号（如需）
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES  # Cookie登录时使用
                )
                # 执行登录流程
                await login_obj.begin()
                # 登录后更新客户端Cookie
                await self.dy_client.update_cookies(browser_context=self.browser_context)
            
            # 6. 设置爬虫类型
            crawler_type_var.set(config.CRAWLER_TYPE)
            
            # 7. 根据配置执行不同的爬虫任务
            if config.CRAWLER_TYPE == "search":
                # 搜索模式：搜索视频并获取评论
                await self.search()
            elif config.CRAWLER_TYPE == "detail":
                # 详情模式：获取指定视频的信息和评论
                await self.get_specified_awemes()
            elif config.CRAWLER_TYPE == "creator":
                # 创作者模式：获取指定创作者的信息和视频
                await self.get_creators_and_videos()

            # 8. 爬虫任务完成
            utils.logger.info("[DouYinCrawler.start] Douyin Crawler finished ...")

    async def search(self) -> None:
        utils.logger.info("[DouYinCrawler.search] Begin search douyin keywords")
        
        # 抖音每页固定返回10条数据
        dy_limit_count = 10  
        # 如果配置的最大抓取数小于每页数量，调整为每页数量
        if config.CRAWLER_MAX_NOTES_COUNT < dy_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = dy_limit_count
        
        start_page = config.START_PAGE  # 起始页码
        
        # 遍历所有关键词（支持多关键词，用逗号分隔）
        for keyword in config.KEYWORDS.split(","):
            source_keyword_var.set(keyword)  # 设置当前关键词到上下文
            utils.logger.info(f"[DouYinCrawler.search] Current keyword: {keyword}")
            
            aweme_list: List[str] = []  # 存储视频ID列表
            page = 0  # 当前页码
            dy_search_id = ""  # 抖音搜索ID，用于翻页
            
            # 循环获取数据，直到达到配置的最大数量
            while (page - start_page + 1) * dy_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                # 跳过起始页之前的页码
                if page < start_page:
                    utils.logger.info(f"[DouYinCrawler.search] Skip {page}")
                    page += 1
                    continue
                
                try:
                    utils.logger.info(f"[DouYinCrawler.search] search douyin keyword: {keyword}, page: {page}")
  
                    # 添加请求延迟
                    await self.add_request_delay(f"搜索关键词 '{keyword}' 第{page}页")
                    
                    # 调用API搜索
                    posts_res = await self.dy_client.search_info_by_keyword(keyword=keyword,
                                                                            offset=page * dy_limit_count - dy_limit_count,
                                                                            publish_time=PublishTimeType(config.PUBLISH_TIME_TYPE),
                                                                            search_id=dy_search_id
                                                                            )
                    if posts_res.get("data") is None or posts_res.get("data") == []:
                        utils.logger.info(f"[DouYinCrawler.search] search douyin keyword: {keyword}, page: {page} is empty,{posts_res.get('data')}`")
                        break

                except DataFetchError:
                    # 搜索失败，跳出循环
                    utils.logger.error(f"[DouYinCrawler.search] search douyin keyword: {keyword} failed")
                    break

                page += 1
                
                # 检查返回数据是否正常
                if "data" not in posts_res:
                    utils.logger.error(
                        f"[DouYinCrawler.search] search douyin keyword: {keyword} failed，账号也许被风控了。")
                    break
                
                # 获取下一页搜索ID
                dy_search_id = posts_res.get("extra", {}).get("logid", "")
                
                # 处理返回的视频列表
                for post_item in posts_res.get("data"):
                    try:
                        # 获取视频信息（支持普通视频和合集视频）
                        aweme_info: Dict = post_item.get("aweme_info") or \
                                         post_item.get("aweme_mix_info", {}).get("mix_items")[0]
                    except TypeError:
                        continue
                    
                    # 保存视频ID
                    aweme_list.append(aweme_info.get("aweme_id", ""))
                    # 更新视频信息到存储
                    await douyin_store.update_douyin_aweme(aweme_item=aweme_info)
                
            # 打印当前关键词获取的所有视频ID
            utils.logger.info(f"[DouYinCrawler.search] keyword:{keyword}, aweme_list:{aweme_list}")
            # 批量获取视频评论
            await self.batch_get_note_comments(aweme_list)

    async def get_specified_awemes(self):
        """获取指定视频的信息和评论
        1. 创建信号量控制并发数
        2. 并发获取所有指定视频的详情
        3. 保存视频信息
        4. 获取所有视频的评论
        """
        # 创建信号量来限制并发请求数
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        # 为每个视频ID创建获取详情的任务
        task_list = [
            self.get_aweme_detail(aweme_id=aweme_id, semaphore=semaphore) for aweme_id in config.DY_SPECIFIED_ID_LIST
        ]
        # 并发执行所有任务
        aweme_details = await asyncio.gather(*task_list)
        # 保存有效的视频信息
        for aweme_detail in aweme_details:
            if aweme_detail is not None:
                await douyin_store.update_douyin_aweme(aweme_detail)
        # 获取所有视频的评论
        await self.batch_get_note_comments(config.DY_SPECIFIED_ID_LIST)

    async def get_aweme_detail(self, aweme_id: str, semaphore: asyncio.Semaphore) -> Any:
        """获取单个视频的详细信息
        使用信号量控制并发，处理可能的异常情况
        """
        async with semaphore:  # 使用信号量控制并发
            try:
                # 添加请求延迟
                await self.add_request_delay(f"获取视频详情 ID:{aweme_id}")
                
                return await self.dy_client.get_video_by_id(aweme_id)
            except DataFetchError as ex:
                # 处理数据获取错误
                utils.logger.error(f"[DouYinCrawler.get_aweme_detail] Get aweme detail error: {ex}")
                return None
            except KeyError as ex:
                # 处理视频不存在的情况
                utils.logger.error(
                    f"[DouYinCrawler.get_aweme_detail] have not fund note detail aweme_id:{aweme_id}, err: {ex}")
                return None

    async def batch_get_note_comments(self, aweme_list: List[str]) -> None:
        """批量获取视频评论
        1. 检查是否启用评论获取功能
        2. 为每个视频创建获取评论的任务
        3. 并发执行所有任务
        """
        # 检查是否启用评论获取功能
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(f"[DouYinCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return

        task_list: List[Task] = []
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        # 为每个视频创建获取评论的任务
        for aweme_id in aweme_list:
            task = asyncio.create_task(
                self.get_comments(aweme_id, semaphore), name=aweme_id)
            task_list.append(task)
        # 如果有任务则等待所有任务完成    
        if len(task_list) > 0:
            await asyncio.wait(task_list)

    async def get_comments(self, aweme_id: str, semaphore: asyncio.Semaphore) -> None:
        """获取单个视频的评论数据
        参数:
            aweme_id: 视频ID
            semaphore: 用于控制并发的信号量
        """
        async with semaphore:  # 使用信号量控制并发访问
            try:
                # 将关键词列表传递给 get_aweme_all_comments 方法
                await self.dy_client.get_aweme_all_comments(
                    aweme_id=aweme_id,
                    crawl_interval=random.random(),  # 随机延迟，避免频繁请求
                    is_fetch_sub_comments=config.ENABLE_GET_SUB_COMMENTS,  # 是否获取子评论
                    callback=douyin_store.batch_update_dy_aweme_comments,  # 评论数据保存的回调函数
                    max_count=config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES  # 单个视频最大评论获取数
                )
                utils.logger.info(
                    f"[DouYinCrawler.get_comments] aweme_id: {aweme_id} comments have all been obtained and filtered ...")
            except DataFetchError as e:
                # 记录评论获取失败的错误
                utils.logger.error(f"[DouYinCrawler.get_comments] aweme_id: {aweme_id} get comments failed, error: {e}")

    async def get_creators_and_videos(self) -> None:
        """获取指定创作者的信息和视频数据
        流程:
        1. 遍历配置的创作者ID列表
        2. 获取每个创作者的基本信息
        3. 获取创作者的所有视频
        4. 获取所有视频的评论
        """
        utils.logger.info("[DouYinCrawler.get_creators_and_videos] Begin get douyin creators")
        for user_id in config.DY_CREATOR_ID_LIST:
            
            # 获取创作者信息
            creator_info: Dict = await self.dy_client.get_user_info(user_id)
            if creator_info:
                # 保存创作者信息到数据库
                await douyin_store.save_creator(user_id, creator=creator_info)

            # 添加请求延迟
            await self.add_request_delay(f"获取创作者所有视频 ID:{user_id}")
            
            # 获取创作者的所有视频信息
            all_video_list = await self.dy_client.get_all_user_aweme_posts(
                sec_user_id=user_id,
                callback=self.fetch_creator_video_detail  # 处理视频详情的回调函数
            )

            # 提取视频ID列表并获取评论
            video_ids = [video_item.get("aweme_id") for video_item in all_video_list]
            await self.batch_get_note_comments(video_ids)

    async def fetch_creator_video_detail(self, video_list: List[Dict]):
        """并发获取视频列表的详细信息并保存
        参数:
            video_list: 视频信息列表
        流程:
        1. 创建信号量控制并发
        2. 为每个视频创建获取详情的任务
        3. 并发执行所有任务
        4. 保存获取到的视频详情
        """
        # 创建信号量控制并发数
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        # 创建获取视频详情的任务列表
        task_list = [
            self.get_aweme_detail(post_item.get("aweme_id"), semaphore) for post_item in video_list
        ]

        # 并发执行所有任务
        note_details = await asyncio.gather(*task_list)
        # 保存有效的视频详情
        for aweme_item in note_details:
            if aweme_item is not None:
                await douyin_store.update_douyin_aweme(aweme_item)

    @staticmethod
    def format_proxy_info(ip_proxy_info: IpInfoModel) -> Tuple[Optional[Dict], Optional[Dict]]:
        """格式化代理信息，生成不同客户端所需的代理配置
        参数:
            ip_proxy_info: 代理IP信息模型
        返回:
            Tuple[Optional[Dict], Optional[Dict]]: 
            - playwright格式的代理配置
            - httpx格式的代理配置
        """
        # 格式化Playwright所需的代理配置
        playwright_proxy = {
            "server": f"{ip_proxy_info.protocol}{ip_proxy_info.ip}:{ip_proxy_info.port}",
            "username": ip_proxy_info.user,
            "password": ip_proxy_info.password,
        }
        # 格式化HTTPX所需的代理配置
        httpx_proxy = {
            f"{ip_proxy_info.protocol}": f"http://{ip_proxy_info.user}:{ip_proxy_info.password}@{ip_proxy_info.ip}:{ip_proxy_info.port}"
        }
        return playwright_proxy, httpx_proxy

    async def create_douyin_client(self, httpx_proxy: Optional[str]) -> DOUYINClient:
        """创建抖音客户端实例
        参数:
            httpx_proxy: 可选的HTTP代理配置
        返回:
            DOUYINClient: 配置好的抖音客户端实例
        流程:
        1. 获取并转换浏览器cookie
        2. 配置请求头信息
        3. 创建客户端实例
        """
        # 获取并转换浏览器cookie为字符串和字典格式
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())  # type: ignore
        
        # 创建抖音客户端实例
        douyin_client = DOUYINClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": await self.context_page.evaluate("() => navigator.userAgent"),  # 获取当前浏览器UA
                "Cookie": cookie_str,
                "Host": "www.douyin.com",
                "Origin": "https://www.douyin.com/",
                "Referer": "https://www.douyin.com/",
                "Content-Type": "application/json;charset=UTF-8"
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return douyin_client

    async def launch_browser(
            self,
            chromium: BrowserType,
            playwright_proxy: Optional[Dict],
            user_agent: Optional[str],
            headless: bool = True
    ) -> BrowserContext:
        """启动浏览器并创建上下文
        参数:
            chromium: Playwright的Chromium实例
            playwright_proxy: 代理配置
            user_agent: 自定义UA
            headless: 是否无头模式运行
        返回:
            BrowserContext: 浏览器上下文实例
        """
        if config.SAVE_LOGIN_STATE:
            # 使用持久化上下文，保存登录状态
            user_data_dir = os.path.join(os.getcwd(), "browser_data",
                                         config.USER_DATA_DIR % config.PLATFORM)  # type: ignore
            browser_context = await chromium.launch_persistent_context(
                user_data_dir=user_data_dir,  # 持久化数据目录
                accept_downloads=True,  # 允许下载
                headless=headless,  # 是否无头模式
                proxy=playwright_proxy,  # type: ignore
                viewport={"width": 1920, "height": 1080},  # 视窗大小
                user_agent=user_agent  # 自定义UA
            )  # type: ignore
            return browser_context
        else:
            # 创建临时上下文
            browser = await chromium.launch(headless=headless, proxy=playwright_proxy)  # type: ignore
            browser_context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )
            return browser_context

    async def close(self) -> None:
        """关闭浏览器上下文，清理资源"""
        await self.browser_context.close()
        utils.logger.info("[DouYinCrawler.close] Browser context closed ...")
