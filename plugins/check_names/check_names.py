# -*- coding: utf-8 -*-
import json
import os
import re
import time
import threading
import math # 用于计算批次数

# 使用 GewechatClient
from lib.gewechat.client import GewechatClient

import plugins # 显式导入 plugins 对象
from plugins import Plugin, Event, EventContext, EventAction # 显式导入基类和事件相关类
from bridge.context import ContextType
from bridge.reply import Reply, ReplyType
from channel.chat_message import ChatMessage
from common import const
from common.log import logger
from config import conf, load_config

# 默认配置
DEFAULT_CONFIG = {
    "rule_pattern": "^[A-Z][a-zA-Z]*(?: [A-Z][a-zA-Z]*)* [\\u4e00-\\u9fa5]+$",
    "check_group_list": [],
    "notify_template": "@{nickname} 请修改你的昵称，例如：Leo 王二",
    "enable_check_all_groups": True,
    "api_request_delay": 1, # API请求间隔，单位秒
    "batch_size": 5,        # 每批处理的群组数量
    "batch_delay_seconds": 300 # 每个批次之间的延迟时间（秒）
}

@plugins.register(
    name="CheckNames",
    desc="定期检查群成员昵称是否符合规范（支持批处理）",
    version="0.3", # 版本更新
    author="Roo",
    desire_priority=10
)
class CheckNames(Plugin):
    def __init__(self):
        super().__init__()
        self.config = {}
        self.client = None
        self.app_id = None
        self.load_config() # 加载配置并初始化 client 和 app_id
        logger.info(f"[CheckNames] inited. Gewechat client initialized: {self.client is not None}, App ID set: {self.app_id is not None}")

    def load_config(self):
        """加载插件配置并初始化 GewechatClient"""
        try:
            config_path = os.path.join(os.path.dirname(__file__), "config.json")
            if not os.path.exists(config_path):
                logger.info(f"[CheckNames] config.json not found, creating from template.")
                template_path = config_path + ".template"
                if os.path.exists(template_path):
                    with open(template_path, "r", encoding="utf-8") as f_template:
                        default_config_template = json.load(f_template)
                    with open(config_path, "w", encoding="utf-8") as f_config:
                        json.dump(default_config_template, f_config, indent=4, ensure_ascii=False)
                    self.config = default_config_template
                else:
                    logger.warning(f"[CheckNames] config.json.template not found. Using default internal config initially.")
                    self.config = DEFAULT_CONFIG.copy()
            else:
                with open(config_path, "r", encoding="utf-8") as f:
                    self.config = json.load(f)

            # --- 配置加载优先级 (插件 > 全局 > 默认) ---
            plugin_base_url = self.config.get("gewechat_base_url")
            plugin_token = self.config.get("gewechat_token")
            plugin_app_id = self.config.get("gewechat_app_id")

            global_base_url = conf().get("gewechat_base_url")
            global_token = conf().get("gewechat_token")
            global_app_id = conf().get("gewechat_app_id")

            final_base_url = plugin_base_url or global_base_url
            final_token = plugin_token or global_token
            final_app_id = plugin_app_id or global_app_id

            # --- 初始化 GewechatClient ---
            if final_base_url and final_token:
                self.client = GewechatClient(final_base_url, final_token)
                logger.info(f"[CheckNames] GewechatClient initialized with base_url: {final_base_url}")
            else:
                 logger.error("[CheckNames] gewechat_base_url or gewechat_token is missing. GewechatClient cannot be initialized.")
                 self.client = None

            # --- 设置 App ID ---
            if final_app_id:
                self.app_id = final_app_id
                self.config["gewechat_app_id"] = final_app_id # 存回配置字典供其他地方使用
                logger.info(f"[CheckNames] Gewechat App ID set to: {self.app_id}")
            else:
                logger.error("[CheckNames] gewechat_app_id is missing. Plugin cannot function correctly.")
                self.app_id = None

            # 检查并补充其他缺失的配置项
            # 使用 setdefault 来简化补充默认值的过程
            for key, value in DEFAULT_CONFIG.items():
                self.config.setdefault(key, value)
                # 记录一下哪些是用了默认值的
                if key not in ["gewechat_base_url", "gewechat_token", "gewechat_app_id"] and self.config[key] == value:
                     # 避免在配置文件已存在但值与默认值相同时重复打印警告
                     config_file_exists = os.path.exists(config_path)
                     try:
                         with open(config_path, "r", encoding="utf-8") as f_check:
                             existing_config = json.load(f_check)
                         if key not in existing_config:
                              logger.warning(f"[CheckNames] Config item '{key}' not found, using default value: {value}")
                     except: # 文件不存在或读取失败时，也用默认值
                          if not config_file_exists:
                               logger.warning(f"[CheckNames] Config item '{key}' not found, using default value: {value}")


        except Exception as e:
            logger.error(f"[CheckNames] Error loading config or initializing client: {e}", exc_info=True)
            self.config = DEFAULT_CONFIG.copy()
            self.client = None
            self.app_id = None

    def _get_all_groups(self):
        """获取所有已保存到通讯录的群聊列表 (使用 GewechatClient)"""
        if not self.client or not self.app_id:
            logger.error("[CheckNames] Gewechat client or App ID not initialized. Cannot fetch contacts.")
            return []

        logger.info("[CheckNames] Fetching group list using GewechatClient...")
        try:
            response = self.client.fetch_contacts_list(self.app_id)
            if response and response.get("ret") == 200:
                data = response.get("data")
                if data and isinstance(data, list):
                    # 主要逻辑：处理返回联系人列表的情况
                    groups = [contact for contact in data if contact.get("wxid", "").endswith("@chatroom")]
                    logger.info(f"[CheckNames] Found {len(groups)} groups in contacts list.")
                    return groups
                elif data and isinstance(data, dict) and 'chatrooms' in data: # 修正键名并处理返回 wxid 列表的情况
                     chatroom_wxids = data['chatrooms']
                     logger.info(f"[CheckNames] Found {len(chatroom_wxids)} chatroom wxids in response.")
                     # 将 wxid 列表转换为后续代码期望的字典列表格式
                     # 注意：这里缺少 nickName，后续 run_check 会使用 wxid 作为 group_name
                     groups = [{"wxid": wxid} for wxid in chatroom_wxids]
                     return groups
                else:
                    # 记录未预期的格式，但返回空列表
                    logger.warning(f"[CheckNames] Unexpected data format in fetchContactsList response: {data}")
                    return []
            else:
                logger.error(f"[CheckNames] fetch_contacts_list API call failed: {response}")
                return []
        except Exception as e:
            logger.error(f"[CheckNames] Error calling fetch_contacts_list: {e}", exc_info=True)
            return []

    def _get_group_members(self, group_wxid):
        """获取指定群聊的成员列表 (使用 GewechatClient)"""
        if not self.client or not self.app_id:
            logger.error("[CheckNames] Gewechat client or App ID not initialized. Cannot fetch group members.")
            return []

        logger.debug(f"[CheckNames] Fetching members for group: {group_wxid} using GewechatClient...")
        try:
            response = self.client.get_chatroom_member_list(self.app_id, group_wxid)
            if response and response.get("ret") == 200:
                data = response.get("data")
                if data and "memberList" in data:
                    return data["memberList"]
                else:
                    logger.warning(f"[CheckNames] 'memberList' not found in get_chatroom_member_list response data for group {group_wxid}: {data}")
                    return []
            else:
                logger.error(f"[CheckNames] get_chatroom_member_list API call failed for group {group_wxid}: {response}")
                return []
        except Exception as e:
            logger.error(f"[CheckNames] Error calling get_chatroom_member_list for group {group_wxid}: {e}", exc_info=True)
            return []

    def _check_nickname(self, nickname):
        """检查昵称是否符合规范"""
        pattern = self.config.get("rule_pattern")
        if not pattern:
            logger.warning("[CheckNames] Nickname rule pattern is not configured.")
            return True
        try:
            if not isinstance(nickname, str):
                logger.warning(f"[CheckNames] Received non-string nickname: {nickname} (type: {type(nickname)}). Treating as non-compliant.")
                return False
            return re.match(pattern, nickname) is not None
        except re.error as e:
            logger.error(f"[CheckNames] Invalid regex pattern: {pattern}, error: {e}")
            return True
        except Exception as e:
             logger.error(f"[CheckNames] Error during nickname check for '{nickname}': {e}", exc_info=True)
             return True

    def _send_notification(self, group_wxid, members_to_notify):
        """向指定群组发送合并的不合规通知 (在群内@, 使用 GewechatClient)
           members_to_notify: list of dicts, each {'wxid': str, 'name_to_at': str}
        """
        if not self.client or not self.app_id:
            logger.error("[CheckNames] Gewechat client or App ID not initialized. Cannot send notification.")
            return
        if not members_to_notify:
            logger.warning("[CheckNames] _send_notification called with empty member list.")
            return

        # 提取 wxid 列表和用于 @ 的名称列表
        wxids_to_at = [member['wxid'] for member in members_to_notify]
        names_to_at_str = " ".join([f"@{member['name_to_at']}" for member in members_to_notify])

        # 获取通知模板的基础文本（移除占位符）
        template = self.config.get("notify_template", "@{nickname} 请修改你的昵称，例如：Leo 王二")
        base_message = template.replace("@{nickname}", "").strip() # 移除占位符以获得基础消息

        # 组合最终消息
        final_message = f"{names_to_at_str} {base_message}"

        logger.info(f"[CheckNames] Sending combined notification to {len(wxids_to_at)} members in group {group_wxid}: {final_message}")
        logger.debug(f"[CheckNames] Members to @ (wxids): {wxids_to_at}")

        try:
            # 将 wxid 列表转换为逗号分隔的字符串
            ats_string = ",".join(wxids_to_at)
            logger.debug(f"[CheckNames] Formatted ats string: {ats_string}")
            # 使用 post_text 发送合并消息，ats 参数传入转换后的字符串
            response = self.client.post_text(self.app_id, group_wxid, final_message, ats=ats_string)
            if response and response.get("ret") == 200:
                logger.info(f"[CheckNames] Combined notification sent successfully to group {group_wxid}.")
            else:
                logger.error(f"[CheckNames] post_text API call failed for combined notification in {group_wxid}: {response}")
        except Exception as e:
            logger.error(f"[CheckNames] Error calling post_text for combined notification in {group_wxid}: {e}", exc_info=True)

        time.sleep(self.config.get("api_request_delay", 1)) # 发送后延时

    def run_check(self):
        """执行昵称检查的主要逻辑 (包含批处理)"""
        logger.info("[CheckNames] Starting nickname check task with batch processing...")
        if not self.client or not self.app_id:
             logger.error("[CheckNames] Gewechat client or App ID not initialized in run_check. Aborting.")
             return

        # 1. 获取目标群组列表
        target_groups = []
        if self.config.get("enable_check_all_groups", True):
            all_groups = self._get_all_groups()
            if all_groups:
                target_groups = all_groups
        else:
            configured_groups = self.config.get("check_group_list", [])
            if not configured_groups:
                logger.info("[CheckNames] No specific groups configured and check_all_groups is disabled. No groups to check.")
                return
            all_groups = self._get_all_groups()
            if not all_groups:
                 logger.error("[CheckNames] Failed to get group list to match configured group names.")
                 return
            group_map = {g.get("nickName"): g.get("wxid") for g in all_groups if g.get("nickName") and g.get("wxid")}
            for group_name in configured_groups:
                if group_name in group_map:
                    target_groups.append({"wxid": group_map[group_name], "nickName": group_name})
                else:
                    logger.warning(f"[CheckNames] Configured group '{group_name}' not found in contacts or has no nickname.")

        if not target_groups:
            logger.info("[CheckNames] No target groups found to check.")
            return

        # 2. 获取批处理配置
        batch_size = self.config.get("batch_size", 5)
        batch_delay_seconds = self.config.get("batch_delay_seconds", 300)
        api_request_delay = self.config.get("api_request_delay", 1)
        total_groups = len(target_groups)
        num_batches = math.ceil(total_groups / batch_size)

        logger.info(f"[CheckNames] Starting check for {total_groups} groups in {num_batches} batches (size: {batch_size}, delay: {batch_delay_seconds}s).")
        total_non_compliant = 0

        # 3. 循环处理批次
        for i in range(num_batches):
            batch_start_index = i * batch_size
            batch_end_index = batch_start_index + batch_size
            current_batch = target_groups[batch_start_index:batch_end_index]
            batch_num = i + 1

            logger.info(f"[CheckNames] Processing batch {batch_num}/{num_batches} ({len(current_batch)} groups)...")

            # 4. 处理当前批次内的群组
            for group in current_batch:
                group_wxid = group.get("wxid")
                group_name = group.get("nickName", group_wxid) # 如果只有wxid，用wxid作为名字
                if not group_wxid:
                    logger.warning(f"[CheckNames] Skipping group with missing wxid in batch {batch_num}: {group}")
                    continue

                logger.info(f"[CheckNames] [Batch {batch_num}] Checking group: {group_name} ({group_wxid})")
                members = self._get_group_members(group_wxid)
                if not members:
                    logger.warning(f"[CheckNames] [Batch {batch_num}] No members found or failed to fetch members for group: {group_name}")
                    time.sleep(api_request_delay) # 获取失败也短暂延时
                    continue

                non_compliant_count_in_group = 0
                non_compliant_members_in_group = [] # 初始化列表以收集不合规成员

                for member in members:
                    if not isinstance(member, dict):
                        logger.warning(f"[CheckNames] [Batch {batch_num}] Skipping invalid member data in group {group_name}: {member}")
                        continue

                    display_name = member.get("displayName")
                    nick_name = member.get("nickName")
                    user_wxid = member.get("wxid")

                    if not user_wxid:
                        logger.debug(f"[CheckNames] [Batch {batch_num}] Skipping member with missing wxid in group {group_name}: {member}")
                        continue

                    is_compliant = False
                    checked_name_type = "" # 'displayName' or 'nickName' or 'None'

                    # 1. 检查 displayName
                    if display_name:
                        checked_name_type = "displayName"
                        if self._check_nickname(display_name):
                            is_compliant = True
                            logger.debug(f"[CheckNames] [Batch {batch_num}] Compliant displayName found: '{display_name}' (wxid: {user_wxid})")
                        else:
                            is_compliant = False
                            logger.warning(f"[CheckNames] [Batch {batch_num}] Non-compliant displayName found: '{display_name}' (wxid: {user_wxid}, nickName: {nick_name})")
                    else:
                        # 2. displayName 为空，检查 nickName
                        logger.debug(f"[CheckNames] [Batch {batch_num}] displayName is empty for {user_wxid}, checking nickName: '{nick_name}'")
                        if nick_name:
                            checked_name_type = "nickName"
                            if self._check_nickname(nick_name):
                                is_compliant = True
                                logger.debug(f"[CheckNames] [Batch {batch_num}] Compliant nickName found (used as fallback): '{nick_name}' (wxid: {user_wxid})")
                            else:
                                is_compliant = False
                                logger.info(f"[CheckNames] [Batch {batch_num}] Non-compliant nickName found (used as fallback): '{nick_name}' (wxid: {user_wxid})") # 改为 INFO
                        else:
                            checked_name_type = "None"
                            is_compliant = False
                            logger.warning(f"[CheckNames] [Batch {batch_num}] Both displayName and nickName are empty for wxid: {user_wxid}")

                    # 4. 如果最终判定不合规，收集信息
                    if not is_compliant:
                        non_compliant_count_in_group += 1
                        total_non_compliant += 1
                        name_to_at = nick_name or user_wxid # @ 时优先用微信昵称
                        non_compliant_members_in_group.append({'wxid': user_wxid, 'name_to_at': name_to_at})
                        logger.info(f"[CheckNames] [Batch {batch_num}] Member {user_wxid} determined non-compliant based on {checked_name_type}. Added to notification list.")

                # 5. 检查完群内所有成员后，如果列表不为空，则发送合并通知
                if non_compliant_members_in_group:
                    logger.info(f"[CheckNames] [Batch {batch_num}] Sending combined notification for {len(non_compliant_members_in_group)} members in group {group_name}.")
                    self._send_notification(group_wxid, non_compliant_members_in_group) # 传递列表
                else:
                    logger.info(f"[CheckNames] [Batch {batch_num}] No non-compliant members found in group {group_name}.")


                logger.info(f"[CheckNames] [Batch {batch_num}] Finished checking group: {group_name}. Found {non_compliant_count_in_group} non-compliant nicknames.")
                time.sleep(api_request_delay) # 每个群检查完后短暂延时

            logger.info(f"[CheckNames] Finished processing batch {batch_num}/{num_batches}.")

            # 5. 批次间延迟（如果不是最后一批）
            if batch_num < num_batches:
                logger.info(f"[CheckNames] Delaying for {batch_delay_seconds} seconds before next batch...")
                time.sleep(batch_delay_seconds)

        logger.info(f"[CheckNames] Nickname check task finished. Processed {total_groups} groups in {num_batches} batches. Total non-compliant nicknames found: {total_non_compliant}")

    def run_scheduled_task(self):
        """供 difytimetask 调用的入口方法"""
        # 在新线程中运行检查，避免阻塞 difytimetask
        thread = threading.Thread(target=self.run_check)
        thread.start()
        logger.info("[CheckNames] Scheduled task triggered, running check in background thread.")
        return "CheckNames 任务已开始在后台执行。"

    def get_help_text(self, **kwargs):
        return "CheckNames 插件：用于定时检查群成员昵称规范性（支持批处理），通过 difytimetask 插件配置定时执行。"