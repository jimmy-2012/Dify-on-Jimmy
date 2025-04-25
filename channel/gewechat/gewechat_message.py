import base64
import uuid
import re
from bridge.context import ContextType
from channel.chat_message import ChatMessage
from common.log import logger
from common.tmp_dir import TmpDir
from config import conf
from lib.gewechat import GewechatClient
import requests
import xml.etree.ElementTree as ET

# 私聊信息示例
"""
{
    "TypeName": "AddMsg",
    "Appid": "wx_xxx",
    "Data": {
        "MsgId": 177581074,
        "FromUserName": {
            "string": "wxid_fromuser"
        },
        "ToUserName": {
            "string": "wxid_touser"
        },
        "MsgType": 49,
        "Content": {
            "string": ""
        },
        "Status": 3,
        "ImgStatus": 1,
        "ImgBuf": {
            "iLen": 0
        },
        "CreateTime": 1733410112,
        "MsgSource": "<msgsource>xx</msgsource>\n",
        "PushContent": "xxx",
        "NewMsgId": 5894648508580188926,
        "MsgSeq": 773900156
    },
    "Wxid": "wxid_gewechat_bot"  // 使用gewechat登录的机器人wxid
}
"""

# 群聊信息示例
"""
{
    "TypeName": "AddMsg",
    "Appid": "wx_xxx",
    "Data": {
        "MsgId": 585326344,
        "FromUserName": {
            "string": "xxx@chatroom"
        },
        "ToUserName": {
            "string": "wxid_gewechat_bot" // 接收到此消息的wxid, 即使用gewechat登录的机器人wxid
        },
        "MsgType": 1,
        "Content": {
            "string": "wxid_xxx:\n@name msg_content" // 发送消息人的wxid和消息内容(包含@name)
        },
        "Status": 3,
        "ImgStatus": 1,
        "ImgBuf": {
            "iLen": 0
        },
        "CreateTime": 1733447040,
        "MsgSource": "<msgsource>\n\t<atuserlist><![CDATA[,wxid_wvp31dkffyml19]]></atuserlist>\n\t<pua>1</pua>\n\t<silence>0</silence>\n\t<membercount>3</membercount>\n\t<signature>V1_cqxXBat9|v1_cqxXBat9</signature>\n\t<tmp_node>\n\t\t<publisher-id></publisher-id>\n\t</tmp_node>\n</msgsource>\n",
        "PushContent": "xxx在群聊中@了你",
        "NewMsgId": 8449132831264840264,
        "MsgSeq": 773900177
    },
    "Wxid": "wxid_gewechat_bot"  // 使用gewechat登录的机器人wxid
}
"""

# 群邀请消息示例
"""
{
    "TypeName": "AddMsg",
    "Appid": "wx_xxx",
    "Data": {
        "MsgId": 488566999,
        "FromUserName": {
            "string": "xxx@chatroom"
        },
        "ToUserName": {
            "string": "wxid_gewechat_bot"
        },
        "MsgType": 10002,
        "Content": {
            "string": "53760920521@chatroom:\n<sysmsg type=\"sysmsgtemplate\">\n\t<sysmsgtemplate>\n\t\t<content_template type=\"tmpl_type_profile\">\n\t\t\t<plain><![CDATA[]]></plain>\n\t\t\t<template><![CDATA[\"$username$\"邀请\"$names$\"加入了群聊]]></template>\n\t\t\t<link_list>\n\t\t\t\t<link name=\"username\" type=\"link_profile\">\n\t\t\t\t\t<memberlist>\n\t\t\t\t\t\t<member>\n\t\t\t\t\t\t\t<username><![CDATA[wxid_eaclcf34ny6221]]></username>\n\t\t\t\t\t\t\t<nickname><![CDATA[刘贺]]></nickname>\n\t\t\t\t\t\t</member>\n\t\t\t\t\t</memberlist>\n\t\t\t\t</link>\n\t\t\t\t<link name=\"names\" type=\"link_profile\">\n\t\t\t\t\t<memberlist>\n\t\t\t\t\t\t<member>\n\t\t\t\t\t\t\t<username><![CDATA[wxid_mmwc3zzkfcl922]]></username>\n\t\t\t\t\t\t\t<nickname><![CDATA[郑德娟]]></nickname>\n\t\t\t\t\t\t</member>\n\t\t\t\t\t</memberlist>\n\t\t\t\t\t<separator><![CDATA[、]]></separator>\n\t\t\t\t</link>\n\t\t\t</link_list>\n\t\t</content_template>\n\t</sysmsgtemplate>\n</sysmsg>\n"
        },
        "Status": 4,
        "ImgStatus": 1,
        "ImgBuf": {
            "iLen": 0
        },
        "CreateTime": 1736820013,
        "MsgSource": "<msgsource>\n\t<tmp_node>\n\t\t<publisher-id></publisher-id>\n\t</tmp_node>\n</msgsource>\n",
        "NewMsgId": 5407479395895269893,
        "MsgSeq": 821038175
    },
    "Wxid": "wxid_gewechat_bot"
}
"""

"""
{
    "TypeName": "ModContacts",
    "Appid": "wx_xxx",
    "Data": {
        "UserName": {
            "string": "xxx@chatroom"
        },
        "NickName": {
            "string": "测试2"
        },
        "PyInitial": {
            "string": "CS2"
        },
        "QuanPin": {
            "string": "ceshi2"
        },
        "Sex": 0,
        "ImgBuf": {
            "iLen": 0
        },
        "BitMask": 4294967295,
        "BitVal": 2,
        "ImgFlag": 1,
        "Remark": {},
        "RemarkPyinitial": {},
        "RemarkQuanPin": {},
        "ContactType": 0,
        "RoomInfoCount": 0,
        "DomainList": [
            {}
        ],
        "ChatRoomNotify": 1,
        "AddContactScene": 0,
        "PersonalCard": 0,
        "HasWeiXinHdHeadImg": 0,
        "VerifyFlag": 0,
        "Level": 0,
        "Source": 0,
        "ChatRoomOwner": "wxid_xxx",
        "WeiboFlag": 0,
        "AlbumStyle": 0,
        "AlbumFlag": 0,
        "SnsUserInfo": {
            "SnsFlag": 0,
            "SnsBgobjectId": 0,
            "SnsFlagEx": 0
        },
        "CustomizedInfo": {
            "BrandFlag": 0
        },
        "AdditionalContactList": {
            "LinkedinContactItem": {}
        },
        "ChatroomMaxCount": 10008,
        "DeleteFlag": 0,
        "Description": "\b\u0004\u0012\u001c\n\u0013wxid_xxx0\u0001@\u0000\u0001\u0000\u0012\u001c\n\u0013wxid_xxx0\u0001@\u0000\u0001\u0000\u0012\u001c\n\u0013wxid_xxx0\u0001@\u0000\u0001\u0000\u0012\u001c\n\u0013wxid_xxx0\u0001@\u0000\u0001\u0000\u0018\u0001\"\u0000(\u00008\u0000",
        "ChatroomStatus": 5,
        "Extflag": 0,
        "ChatRoomBusinessType": 0
    },
    "Wxid": "wxid_xxx"
}
"""

# 群聊中移除用户示例
"""
{
    "UserName": {
        "string": "xxx@chatroom"
    },
    "NickName": {
        "string": "AITestGroup"
    },
    "PyInitial": {
        "string": "AITESTGROUP"
    },
    "QuanPin": {
        "string": "AITestGroup"
    },
    "Sex": 0,
    "ImgBuf": {
        "iLen": 0
    },
    "BitMask": 4294967295,
    "BitVal": 2,
    "ImgFlag": 1,
    "Remark": {},
    "RemarkPyinitial": {},
    "RemarkQuanPin": {},
    "ContactType": 0,
    "RoomInfoCount": 0,
    "DomainList": [
        {}
    ],
    "ChatRoomNotify": 1,
    "AddContactScene": 0,
    "PersonalCard": 0,
    "HasWeiXinHdHeadImg": 0,
    "VerifyFlag": 0,
    "Level": 0,
    "Source": 0,
    "ChatRoomOwner": "wxid_xxx",
    "WeiboFlag": 0,
    "AlbumStyle": 0,
    "AlbumFlag": 0,
    "SnsUserInfo": {
        "SnsFlag": 0,
        "SnsBgobjectId": 0,
        "SnsFlagEx": 0
    },
    "CustomizedInfo": {
        "BrandFlag": 0
    },
    "AdditionalContactList": {
        "LinkedinContactItem": {}
    },
    "ChatroomMaxCount": 10037,
    "DeleteFlag": 0,
    "Description": "\b\u0002\u0012\u001c\n\u0013wxid_eacxxxx\u0001@\u0000�\u0001\u0000\u0012\u001c\n\u0013wxid_xxx\u0001@\u0000�\u0001\u0000\u0018\u0001\"\u0000(\u00008\u0000",
    "ChatroomStatus": 4,
    "Extflag": 0,
    "ChatRoomBusinessType": 0
}
"""

# 群聊中移除用户示例
"""
{
    "TypeName": "ModContacts",
    "Appid": "wx_xxx",
    "Data": {
        "UserName": {
            "string": "xxx@chatroom"
        },
        "NickName": {
            "string": "测试2"
        },
        "PyInitial": {
            "string": "CS2"
        },
        "QuanPin": {
            "string": "ceshi2"
        },
        "Sex": 0,
        "ImgBuf": {
            "iLen": 0
        },
        "BitMask": 4294967295,
        "BitVal": 2,
        "ImgFlag": 2,
        "Remark": {},
        "RemarkPyinitial": {},
        "RemarkQuanPin": {},
        "ContactType": 0,
        "RoomInfoCount": 0,
        "DomainList": [
            {}
        ],
        "ChatRoomNotify": 1,
        "AddContactScene": 0,
        "PersonalCard": 0,
        "HasWeiXinHdHeadImg": 0,
        "VerifyFlag": 0,
        "Level": 0,
        "Source": 0,
        "ChatRoomOwner": "wxid_xxx",
        "WeiboFlag": 0,
        "AlbumStyle": 0,
        "AlbumFlag": 0,
        "SnsUserInfo": {
            "SnsFlag": 0,
            "SnsBgobjectId": 0,
            "SnsFlagEx": 0
        },
        "SmallHeadImgUrl": "https://wx.qlogo.cn/mmcrhead/xxx/0",
        "CustomizedInfo": {
            "BrandFlag": 0
        },
        "AdditionalContactList": {
            "LinkedinContactItem": {}
        },
        "ChatroomMaxCount": 10007,
        "DeleteFlag": 0,
        "Description": "\b\u0003\u0012\u001c\n\u0013wxid_xxx0\u0001@\u0000\u0001\u0000\u0012\u001c\n\u0013wxid_xxx0\u0001@\u0000\u0001\u0000\u0012\u001c\n\u0013wxid_xxx0\u0001@\u0000\u0001\u0000\u0018\u0001\"\u0000(\u00008\u0000",
        "ChatroomStatus": 5,
        "Extflag": 0,
        "ChatRoomBusinessType": 0
    },
    "Wxid": "wxid_xxx"
}
"""

class GeWeChatMessage(ChatMessage):
    def __init__(self, msg, client: GewechatClient):
        super().__init__(msg)
        self.msg = msg
        self.content = ''  # 初始化self.content为空字符串

        # 添加 self.msg_data 属性，兼容 Data 和 data 字段
        self.msg_data = {}
        if 'Data' in msg:
            self.msg_data = msg['Data']
        elif 'data' in msg:
            self.msg_data = msg['data']
        else:
            logger.warning(f"[gewechat] Missing both 'Data' and 'data' in message")
            
        self.create_time = self.msg_data.get('CreateTime', 0)
        if not self.msg_data:
            logger.warning(f"[gewechat] No message data available")
            return
        if 'NewMsgId' not in self.msg_data :
            logger.warning(f"[gewechat] Missing 'NewMsgId' in message data")
            logger.debug(f"[gewechat] msg_data: {self.msg_data}")
            return
        self.msg_id = self.msg_data['NewMsgId']
        self.is_group = True if "@chatroom" in self.msg_data['FromUserName']['string'] else False

        notes_join_group = ["加入群聊", "加入了群聊", "invited", "joined", "移出了群聊"]
        notes_bot_join_group = ["邀请你", "invited you", "You've joined", "你通过扫描"]

        self.client = client
        msg_type = self.msg_data['MsgType']
        self.app_id = conf().get("gewechat_app_id")

        self.from_user_id = self.msg_data['FromUserName']['string']
        self.to_user_id = self.msg_data['ToUserName']['string']
        self.other_user_id = self.from_user_id
        # 检查是否是公众号等非用户账号的消息
        if self._is_non_user_message(self.msg_data.get('MsgSource', ''), self.from_user_id):
            self.ctype = ContextType.NON_USER_MSG
            self.content = self.msg_data.get('Content', {}).get('string', '')  # 确保获取字符串
            logger.debug(f"[gewechat] detected non-user message from {self.from_user_id}: {self.content}")
            return

        if msg_type == 1:  # Text message
            self.ctype = ContextType.TEXT
            self.content = self.msg_data.get('Content', {}).get('string', '')
        elif msg_type == 34:  # Voice message
            self.ctype = ContextType.VOICE
            self.content = self.msg_data.get('Content', {}).get('string', '')
            if 'ImgBuf' in self.msg_data and 'buffer' in self.msg_data['ImgBuf'] and self.msg_data['ImgBuf']['buffer']:
                silk_data = base64.b64decode(self.msg_data['ImgBuf']['buffer'])
                silk_file_name = f"voice_{uuid.uuid4()}.silk"
                silk_file_path = TmpDir().path() + silk_file_name
                with open(silk_file_path, "wb") as f:
                    f.write(silk_data)
                self.content = silk_file_path
        elif msg_type == 3:  # Image message
            self.ctype = ContextType.IMAGE
            self.content = TmpDir().path() + str(self.msg_id) + ".png"
            self._prepare_fn = self.download_image
        elif msg_type == 49:  # 引用消息，小程序，公众号等
            # After getting content_xml
            content_xml = self.msg_data['Content']['string']
            # Find the position of '<?xml' declaration and remove any prefix
            xml_start = content_xml.find('<?xml version=')
            if xml_start != -1:
                content_xml = content_xml[xml_start:]
            try:
                root = ET.fromstring(content_xml)
                appmsg = root.find('appmsg')
                if appmsg is not None:
                    msg_type_node = appmsg.find('type')
                    if msg_type_node is not None and msg_type_node.text == '57':
                        self.ctype = ContextType.TEXT
                        refermsg = appmsg.find('refermsg')
                        if refermsg is not None:
                            displayname = refermsg.find('displayname').text if refermsg.find('displayname') is not None else ''
                            quoted_content = refermsg.find('content').text if refermsg.find('content') is not None else ''
                            title = appmsg.find('title').text if appmsg.find('title') is not None else ''
                            self.content = f"「{displayname}: {quoted_content}」----------\n{title}"
                        else:
                            self.content = content_xml
                    elif msg_type_node is not None and msg_type_node.text == '5':
                        title = appmsg.find('title').text if appmsg.find('title') is not None else "无标题"
                        if "加入群聊" in title:
                            self.ctype = ContextType.TEXT
                            self.content = content_xml
                        else:
                            url = appmsg.find('url').text if appmsg.find('url') is not None else ""
                            self.ctype = ContextType.SHARING
                            self.content = url
                    else:
                        self.ctype = ContextType.TEXT
                        self.content = content_xml
                else:
                    self.ctype = ContextType.TEXT
                    self.content = content_xml
            except ET.ParseError:
                self.ctype = ContextType.TEXT
                self.content = content_xml
        elif msg_type == 51:
            self.ctype = ContextType.STATUS_SYNC
            self.content = self.msg_data.get('Content', {}).get('string', '')
            return
        elif msg_type == 10002 and self.is_group:  # 群系统消息
            content = self.msg_data.get('Content', {}).get('string', '')
            logger.debug(f"[gewechat] detected group system message: {content}")
            
            # --- Start: Add PatPat Handling ---
            try:
                # Attempt to parse XML first to check for 'pat' type
                xml_content_pat = content.split(':\n', 1)[1] if ':\n' in content else content
                root_pat = ET.fromstring(xml_content_pat)
                if root_pat.get('type') == 'pat':
                    pat_elem = root_pat.find('.//pat')
                    if pat_elem is not None:
                        patted_username_elem = pat_elem.find('pattedusername')
                        if patted_username_elem is not None and patted_username_elem.text == self.my_id:
                            logger.debug(f"[gewechat] Detected PATPAT message for self.")
                            self.ctype = ContextType.PATPAT
                            # Extract who patted if needed, e.g., pat_elem.find('fromusername').text
                            # Set content or leave it empty as PATPAT context might not need specific content
                            self.content = "" # Or extract template like "${user}拍了拍我"
                            return # Pat message handled, exit processing for this message
                        else:
                             # It's a pat message, but not for the bot, treat as INFO or ignore
                             logger.debug(f"[gewechat] Detected PATPAT message for others, ignoring.")
                             self.ctype = ContextType.INFO # Or None, depending on desired behavior
                             self.content = content # Keep original content
                             return # Pat message for others handled (ignored), exit processing
            except ET.ParseError:
                # Not a valid XML or not the expected structure, likely not a pat message we handle
                logger.debug("[gewechat] XML parse failed or not a pat message, proceeding to other checks.")
                pass # Continue to check for join group notes etc.
            except Exception as e:
                logger.error(f"[gewechat] Error processing potential pat message: {e}")
                pass # Continue to other checks on error
            # --- End: Add PatPat Handling ---

            # Continue with existing checks if it wasn't a handled pat message
            if any(note in content for note in notes_bot_join_group):
                logger.warn("机器人加入群聊消息，不处理~")
                self.content = content
                return
                
            if any(note in content for note in notes_join_group):
                try:
                    xml_content = content.split(':\n', 1)[1] if ':\n' in content else content
                    root = ET.fromstring(xml_content)
                    
                    sysmsgtemplate = root.find('.//sysmsgtemplate')
                    if sysmsgtemplate is None:
                        raise ET.ParseError("No sysmsgtemplate found")
                        
                    content_template = sysmsgtemplate.find('.//content_template')
                    if content_template is None:
                        raise ET.ParseError("No content_template found")
                        
                    content_type = content_template.get('type')
                    if content_type not in ['tmpl_type_profilewithrevoke', 'tmpl_type_profile']:
                        raise ET.ParseError(f"Invalid content_template type: {content_type}")
                    
                    template = content_template.find('.//template')
                    if template is None:
                        raise ET.ParseError("No template element found")

                    link_list = content_template.find('.//link_list')
                    target_nickname = "未知用户"
                    target_username = None
                    
                    if link_list is not None:
                        # --- Start of Corrected Logic ---
                        inviter_nicknames = []
                        inviter_usernames = []
                        invitee_nicknames = []
                        invitee_usernames = []

                        # Iterate through all links to find relevant parties
                        for action_link in link_list.findall('link'):
                            link_name = action_link.get('name')
                            memberlist = action_link.find('memberlist')
                            if memberlist is not None:
                                for member in memberlist.findall('member'):
                                    nickname_elem = member.find('nickname')
                                    username_elem = member.find('username')
                                    nickname = nickname_elem.text if nickname_elem is not None else "未知用户"
                                    username = username_elem.text if username_elem is not None else None

                                    if link_name == 'username': # Inviter/Operator
                                        inviter_nicknames.append(nickname)
                                        inviter_usernames.append(username)
                                    elif link_name == 'names': # Invitee/Target
                                        invitee_nicknames.append(nickname)
                                        invitee_usernames.append(username)
                        
                        # Process invitee/target nicknames (could be multiple)
                        separator_elem = link_list.find('.//link[@name="names"]/separator')
                        separator = separator_elem.text if separator_elem is not None else '、'
                        target_nickname = separator.join(invitee_nicknames) if invitee_nicknames else "未知用户"
                        target_username = next((u for u in invitee_usernames if u), None) # Get first valid ID

                        # Process inviter/operator nickname (usually one)
                        inviter_nickname = inviter_nicknames[0] if inviter_nicknames else "未知用户"
                        inviter_username = inviter_usernames[0] if inviter_usernames else None

                        # Determine event type based on template content
                        template_elem = content_template.find('.//template') # Ensure template_elem is defined
                        template_text = template_elem.text if template_elem is not None else ""

                        # Construct final message content and type
                        if "邀请" in template_text and "加入" in template_text:
                            self.content = f'"{inviter_nickname}"邀请"{target_nickname}"加入了群聊'
                            self.ctype = ContextType.JOIN_GROUP
                            # For join events, focus on the joiner
                            self.actual_user_nickname = target_nickname
                            self.actual_user_id = target_username
                        elif "移出" in template_text:
                            self.content = f'"{inviter_nickname}"将"{target_nickname}"移出了群聊'
                            self.ctype = ContextType.EXIT_GROUP
                            # For exit events, focus on the leaver
                            self.actual_user_nickname = target_nickname
                            self.actual_user_id = target_username
                        # Add handling for other templates like changing group name if needed
                        # elif "修改群名为" in template_text:
                        #    self.ctype = ContextType.INFO
                        #    self.content = f'"{inviter_nickname}"修改群名为"{...}"' # Extract new name
                        #    self.actual_user_nickname = inviter_nickname
                        #    self.actual_user_id = inviter_username
                        else:
                            logger.warning(f"[gewechat] Unhandled system message template: type={content_type}, template='{template_text}'")
                            self.content = content # Keep original content string
                            self.ctype = ContextType.INFO # Use a generic type
                            self.actual_user_nickname = None # Cannot determine specific user
                            self.actual_user_id = None
                        # --- End of Corrected Logic ---
                    else: # Handle case where link_list is None
                         logger.warning(f"[gewechat] No link_list found in system message: type={msg_type}, content='{content}'")
                         self.content = content # Keep original content string
                         self.ctype = ContextType.INFO # Use a generic type
                         self.actual_user_nickname = None
                         self.actual_user_id = None

                    # This debug log should be outside the if/else for link_list, but inside the try block
                    logger.debug(f"[gewechat] parsed group system message: ctype={self.ctype}, content='{self.content}', "
                                 f"actual_user_id={self.actual_user_id}, actual_user_nickname='{self.actual_user_nickname}'")

                except ET.ParseError as e:
                    logger.error(f"[gewechat] Failed to parse group system message XML: {e}. Content: {content}")
                    self.content = content # Fallback to original content
                    self.ctype = ContextType.INFO # Set a generic type on error
                    self.actual_user_nickname = None
                    self.actual_user_id = None
                except Exception as e:
                    logger.error(f"[gewechat] Unexpected error parsing group system message: {e}. Content: {content}")
                    self.content = content # Fallback to original content
                    self.ctype = ContextType.INFO # Set a generic type on error
                    self.actual_user_nickname = None
                    self.actual_user_id = None
        
        # Ensure handling for other message types remains correct
        elif msg_type == 1: # TEXT
            self.ctype = ContextType.TEXT
            self.content = self.msg_data.get('Content', {}).get('string', '')
            # Add any specific text message parsing if needed
        elif msg_type == 3: # IMAGE
            self.ctype = ContextType.IMAGE
            # Assuming _download_image handles content extraction
            self.content = self._download_image()
        elif msg_type == 34: # VOICE
            self.ctype = ContextType.VOICE
            # Assuming _download_voice handles content extraction
            self.content = self._download_voice()
        elif msg_type == 43: # VIDEO
            self.ctype = ContextType.VIDEO
            # Assuming _download_video handles content extraction
            self.content = self._download_video()
        elif msg_type == 47: # EMOJI
            self.ctype = ContextType.EMOJI
            self.content = self.msg_data.get('Content', {}).get('string', '') # Or handle XML for custom emoji
        elif msg_type == 49: # SHARE / FILE / etc. (Needs specific parsing)
            self.ctype = ContextType.FILE # Default, might need refinement
            self.content = self.msg_data.get('Content', {}).get('string', '') # Basic content
            # Add parsing logic for different appmsg types (file, link, etc.) here
            self._parse_appmsg() # Example call to a new helper method
        # Add elif for other msg_type if necessary
        
        else: # Fallback for truly unsupported types
            logger.warning(f"Unsupported message type ignored: Type:{msg_type}, Content: {self.msg_data.get('Content', {}).get('string', '')[:50]}")
            # Set to a state that can be safely ignored later
            self.ctype = ContextType.INFO
            self.content = "[Unsupported Message Type]"
            # raise NotImplementedError(f"Unsupported message type: Type:{msg_type}") # Avoid raising error to prevent crash

        # 获取群聊或好友的名称 (This part seems fine)
        if self.other_user_id: # Ensure other_user_id exists before fetching info
            try:
                brief_info_response = self.client.get_brief_info(self.app_id, [self.other_user_id])
                if brief_info_response.get('ret') == 200 and brief_info_response.get('data'):
                    brief_info = brief_info_response['data'][0]
                    self.other_user_nickname = brief_info.get('nickName', self.other_user_id)
                else:
                    logger.warning(f"[gewechat] Failed to get brief info for {self.other_user_id}: {brief_info_response}")
            except Exception as e:
                 logger.error(f"[gewechat] Error getting brief info for {self.other_user_id}: {e}")
        if brief_info_response.get('ret') == 200 and brief_info_response.get('data'):
            brief_info = brief_info_response['data'][0]
            self.other_user_nickname = brief_info.get('nickName', self.other_user_id)

        if self.is_group:
            # 如果是群聊消息，获取实际发送者信息
            # 群聊信息结构
            """
            {
                "Data": {
                    "Content": {
                        "string": "wxid_xxx:\n@name msg_content" // 发送消息人的wxid和消息内容(包含@name)
                    }
                }
            }
            """
            # 获取实际发送者wxid

            self.actual_user_id = self.msg_data.get('Content', {}).get('string', '').split(':', 1)[0]
            # 从群成员列表中获取实际发送者信息
            """
            {
                "ret": 200,
                "msg": "操作成功",
                "data": {
                    "memberList": [
                        {
                            "wxid": "",
                            "nickName": "朝夕。",
                            "displayName": null,
                        },
                        {
                            "wxid": "",
                            "nickName": "G",
                            "displayName": "G1",
                        },
                    ]
                }
            }
            """
            chatroom_member_list_response = self.client.get_chatroom_member_list(self.app_id, self.from_user_id)
            if chatroom_member_list_response.get('ret') == 200 and chatroom_member_list_response.get('data', {}).get('memberList'):
                # 从群成员列表中匹配acual_user_id
                for member_info in chatroom_member_list_response['data']['memberList']:
                    if member_info['wxid'] == self.actual_user_id:
                         # 先获取displayName，如果displayName为空，再获取nickName
                        self.actual_user_nickname = member_info.get('displayName') or member_info.get('nickName', self.actual_user_id)
                        break
            self.actual_user_nickname = self.actual_user_nickname or self.actual_user_id

                        # 检查是否被at
            # 群聊at结构
            """
            {
                'Data': {
                    'MsgSource': '<msgsource>\n\t<atuserlist><![CDATA[,wxid_xxx,wxid_xxx]]></atuserlist>\n\t<pua>1</pua>\n\t<silence>0</silence>\n\t<membercount>3</membercount>\n\t<signature>V1_cqxXBat9|v1_cqxXBat9</signature>\n\t<tmp_node>\n\t\t<publisher-id></publisher-id>\n\t</tmp_node>\n</msgsource>\n',
                },
            }
            """
            # 优先从MsgSource的XML中解析是否被at
            msg_source = self.msg_data.get('MsgSource', '')
            self.is_at = False
            xml_parsed = False
            if msg_source:
                try:
                    root = ET.fromstring(msg_source)
                    atuserlist_elem = root.find('atuserlist')
                    if atuserlist_elem is not None and atuserlist_elem.text:
                        self.is_at = self.to_user_id in atuserlist_elem.text
                        xml_parsed = True
                except ET.ParseError:
                    pass
            # 只有在XML解析失败时才从PushContent中判断
            if not xml_parsed:
                self.is_at = '在群聊中@了你' in self.msg_data.get('PushContent', '')
                logger.debug(f"[gewechat] Parse is_at from PushContent. self.is_at: {self.is_at}")
            # 确保self.content是字符串后进行替换
            self.content = str(self.content)
            self.content = re.sub(f'{self.actual_user_id}:\n', '', self.content)
            self.content = re.sub(r'@[^\u2005]+\u2005', '', self.content)
        else:
            # 如果不是群聊消息，保持结构统一，也要设置actual_user_id和actual_user_nickname
            self.actual_user_id = self.other_user_id
            self.actual_user_nickname = self.other_user_nickname

        self.my_msg = self.msg.get('Wxid') == self.from_user_id

    def download_voice(self):
        try:
            voice_data = self.client.download_voice(self.msg['Wxid'], self.msg_id)
            with open(self.content, "wb") as f:
                f.write(voice_data)
        except Exception as e:
            logger.error(f"[gewechat] Failed to download voice file: {e}")

    def download_image(self):
        try:
            try:
                # 尝试下载高清图片
                content_xml = self.msg_data['Content']['string']
                # Find the position of '<?xml' declaration and remove any prefix
                xml_start = content_xml.find('<?xml version=')
                if xml_start != -1:
                    content_xml = content_xml[xml_start:]
                image_info = self.client.download_image(app_id=self.app_id, xml=content_xml, type=1)
            except Exception as e:
                logger.warning(f"[gewechat] Failed to download high-quality image: {e}")
                # 尝试下载普通图片
                image_info = self.client.download_image(app_id=self.app_id, xml=content_xml, type=2)
            if image_info['ret'] == 200 and image_info['data']:
                file_url = image_info['data']['fileUrl']
                logger.info(f"[gewechat] Download image file from {file_url}")
                download_url = conf().get("gewechat_download_url").rstrip('/')
                full_url = download_url + '/' + file_url
                try:
                    file_data = requests.get(full_url).content
                except Exception as e:
                    logger.error(f"[gewechat] Failed to download image file: {e}")
                    return
                with open(self.content, "wb") as f:
                    f.write(file_data)
            else:
                logger.error(f"[gewechat] Failed to download image file: {image_info}")
        except Exception as e:
            logger.error(f"[gewechat] Failed to download image file: {e}")

    def prepare(self):
        if self._prepare_fn:
            self._prepare_fn()

    def _is_non_user_message(self, msg_source: str, from_user_id: str) -> bool:
        """检查消息是否来自非用户账号（如公众号、腾讯游戏、微信团队等）
        
        Args:
            msg_source: 消息的MsgSource字段内容
            from_user_id: 消息发送者的ID
            
        Returns:
            bool: 如果是非用户消息返回True，否则返回False
            
        Note:
            通过以下方式判断是否为非用户消息：
            1. 检查MsgSource中是否包含特定标签
            2. 检查发送者ID是否为特殊账号或以特定前缀开头
        """
        # 检查发送者ID
        special_accounts = ["Tencent-Games", "weixin"]
        if from_user_id in special_accounts or from_user_id.startswith("gh_"):
            logger.debug(f"[gewechat] non-user message detected by sender id: {from_user_id}")
            return True

        # 检查消息源中的标签
        # 示例:<msgsource>\n\t<tips>3</tips>\n\t<bizmsg>\n\t\t<bizmsgshowtype>0</bizmsgshowtype>\n\t\t<bizmsgfromuser><![CDATA[weixin]]></bizmsgfromuser>\n\t</bizmsg>
        non_user_indicators = [
            "<tips>3</tips>",
            "<bizmsgshowtype>",
            "</bizmsgshowtype>",
            "<bizmsgfromuser>",
            "</bizmsgfromuser>"
        ]
        if any(indicator in msg_source for indicator in non_user_indicators):
            logger.debug(f"[gewechat] non-user message detected by msg_source indicators")
            return True

        return False
