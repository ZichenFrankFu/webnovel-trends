# spiders/fanqie_font_decoder.py
import re
import requests
from fontTools.ttLib import TTFont
import base64
from io import BytesIO

dict_data2_xs = {
        '58670': '0', '58413': '1', '58678': '2', '58371': '3', '58353': '4',
        '58480': '5', '58359': '6', '58449': '7', '58540': '8', '58692': '9',
        '58712': 'a', '58542': 'b', '58575': 'c', '58626': 'd', '58691': 'e',
        '58561': 'f', '58362': 'g', '58619': 'h', '58430': 'i', '58531': 'j',
        '58588': 'k', '58440': 'l', '58681': 'm', '58631': 'n', '58376': 'o',
        '58429': 'p', '58555': 'q', '58498': 'r', '58518': 's', '58453': 't',
        '58397': 'u', '58356': 'v', '58435': 'w', '58514': 'x', '58482': 'y',
        '58529': 'z', '58515': 'A', '58688': 'B', '58709': 'C', '58344': 'D',
        '58656': 'E', '58381': 'F', '58576': 'G', '58516': 'H', '58463': 'I',
        '58649': 'J', '58571': 'K', '58558': 'L', '58433': 'M', '58517': 'N',
        '58387': 'O', '58687': 'P', '58537': 'Q', '58541': 'R', '58458': 'S',
        '58390': 'T', '58466': 'U', '58386': 'V', '58697': 'W', '58519': 'X',
        '58511': 'Y', '58634': 'Z', '58611': '的', '58590': '一', '58398': '是',
        '58422': '了', '58657': '我', '58666': '不', '58562': '人', '58345': '在',
        '58510': '他', '58496': '有', '58654': '这', '58441': '个', '58493': '上',
        '58714': '们', '58618': '来', '58528': '到', '58620': '时', '58403': '大',
        '58461': '地', '58481': '为', '58700': '子', '58708': '中', '58503': '你',
        '58442': '说', '58639': '生', '58506': '国', '58663': '年', '58436': '着',
        '58563': '就', '58391': '那', '58357': '和', '58354': '要', '58695': '她',
        '58372': '出', '58696': '也', '58551': '得', '58445': '里', '58408': '后',
        '58599': '自', '58424': '以', '58394': '会', '58348': '家', '58426': '可',
        '58673': '下', '58417': '而', '58556': '过', '58603': '天', '58565': '去',
        '58604': '能', '58522': '对', '58632': '小', '58622': '多', '58350': '然',
        '58605': '于', '58617': '心', '58401': '学', '58637': '么', '58684': '之',
        '58382': '都', '58464': '好', '58487': '看', '58693': '起', '58608': '发',
        '58392': '当', '58474': '没', '58601': '成', '58355': '只', '58573': '如',
        '58499': '事', '58469': '把', '58361': '还', '58698': '用', '58489': '第',
        '58711': '样', '58457': '道', '58635': '想', '58492': '作', '58647': '种',
        '58623': '开', '58521': '美', '58609': '总', '58530': '从', '58665': '无',
        '58652': '情', '58676': '己', '58456': '面', '58581': '最', '58509': '女',
        '58488': '但', '58363': '现', '58685': '前', '58396': '些', '58523': '所',
        '58471': '同', '58485': '日', '58613': '手', '58533': '又', '58589': '行',
        '58527': '意', '58593': '动', '58699': '方', '58707': '期', '58414': '它',
        '58596': '头', '58570': '经', '58660': '长', '58364': '儿', '58526': '回',
        '58501': '位', '58638': '分', '58404': '爱', '58677': '老', '58535': '因',
        '58629': '很', '58577': '给', '58606': '名', '58497': '法', '58662': '间',
        '58479': '斯', '58532': '知', '58380': '世', '58385': '什', '58405': '两',
        '58644': '次', '58578': '使', '58505': '身', '58564': '者', '58412': '被',
        '58686': '高', '58624': '已', '58667': '亲', '58607': '其', '58616': '进',
        '58368': '此', '58427': '话', '58423': '常', '58633': '与', '58525': '活',
        '58543': '正', '58418': '感', '58597': '见', '58683': '明', '58507': '问',
        '58621': '力', '58703': '理', '58438': '尔', '58536': '点', '58384': '文',
        '58484': '几', '58539': '定', '58554': '本', '58421': '公', '58347': '特',
        '58569': '做', '58710': '外', '58574': '孩', '58375': '相', '58645': '西',
        '58592': '果', '58572': '走', '58388': '将', '58370': '月', '58399': '十',
        '58651': '实', '58546': '向', '58504': '声', '58419': '车', '58407': '全',
        '58672': '信', '58675': '重', '58538': '三', '58465': '机', '58374': '工',
        '58579': '物', '58402': '气', '58702': '每', '58553': '并', '58360': '别',
        '58389': '真', '58560': '打', '58690': '太', '58473': '新', '58512': '比',
        '58653': '才', '58704': '便', '58545': '夫', '58641': '再', '58475': '书',
        '58583': '部', '58472': '水', '58478': '像', '58664': '眼', '58586': '等',
        '58568': '体', '58674': '却', '58490': '加', '58476': '电', '58346': '主',
        '58630': '界', '58595': '门', '58502': '利', '58713': '海', '58587': '受',
        '58548': '听', '58351': '表', '58547': '德', '58443': '少', '58460': '克',
        '58636': '代', '58585': '员', '58625': '许', '58694': '稜', '58428': '先',
        '58640': '口', '58628': '由', '58612': '死', '58446': '安', '58468': '写',
        '58410': '性', '58508': '马', '58594': '光', '58483': '白', '58544': '或',
        '58495': '住', '58450': '难', '58643': '望', '58486': '教', '58406': '命',
        '58447': '花', '58669': '结', '58415': '乐', '58444': '色', '58549': '更',
        '58494': '拉', '58409': '东', '58658': '神', '58557': '记', '58602': '处',
        '58559': '让', '58610': '母', '58513': '父', '58500': '应', '58378': '直',
        '58680': '字', '58352': '场', '58383': '平', '58454': '报', '58671': '友',
        '58668': '关', '58452': '放', '58627': '至', '58400': '张', '58455': '认',
        '58416': '接', '58552': '告', '58614': '入', '58582': '笑', '58534': '内',
        '58701': '英', '58349': '军', '58491': '候', '58467': '民', '58365': '岁',
        '58598': '往', '58425': '何', '58462': '度', '58420': '山', '58661': '觉',
        '58615': '路', '58648': '带', '58470': '万', '58377': '男', '58520': '边',
        '58646': '风', '58600': '解', '58431': '叫', '58715': '任', '58524': '金',
        '58439': '快', '58566': '原', '58477': '吃', '58642': '妈', '58437': '变',
        '58411': '通', '58451': '师', '58395': '立', '58369': '象', '58706': '数',
        '58705': '四', '58379': '失', '58567': '满', '58373': '战', '58448': '远',
        '58659': '格', '58434': '士', '58679': '音', '58432': '轻', '58689': '目',
        '58591': '条', '58682': '呢'
    }

def create_char_mapping(mapping_dict):
    char_map = {}
    for dec_code, char in mapping_dict.items():
        try:
            unicode_int = int(dec_code)
            if 0 <= unicode_int <= 0x10FFFF:
                encrypted_char = chr(unicode_int)
                char_map[encrypted_char] = char
        except ValueError:
            continue
    return char_map

FANQIE_CHAR_MAP = create_char_mapping(dict_data2_xs)

class FontDecoder:
    """字体解密处理器"""

    def __init__(self):
        self.custom_mapping = None
        self.font_cache = {}

    def set_custom_mapping(self, mapping):
        """设置自定义映射表（你提供的dict_data2_xs）"""
        self.custom_mapping = mapping

    def _parse_font_from_html(self, html):
        """从HTML中提取字体文件URL和base64数据"""
        font_urls = []

        # 查找字体URL（woff、woff2、ttf格式）
        url_patterns = [
            r"url\('(https?://[^']+\.woff2?)'\)",
            r'src:url\(([^)]+\.woff2?)\)',
            r'@font-face[^}]+src:url\(([^)]+\.woff2?)[^}]+}',
        ]

        for pattern in url_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)
            font_urls.extend(matches)

        # 查找base64编码的字体
        base64_patterns = [
            r'src:url\(data:application/font-woff2?;base64,([^)]+)\)',
            r"url\('data:application/font-woff2?;base64,([^']+)'\)",
        ]

        base64_fonts = []
        for pattern in base64_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)
            base64_fonts.extend(matches)

        return font_urls, base64_fonts

    def download_font(self, font_url):
        """下载字体文件"""
        if font_url in self.font_cache:
            return self.font_cache[font_url]

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://fanqienovel.com/'
            }
            response = requests.get(font_url, headers=headers, timeout=10)
            font_data = BytesIO(response.content)
            self.font_cache[font_url] = font_data
            return font_data
        except Exception as e:
            print(f"字体下载失败 {font_url}: {e}")
            return None

    def decode_base64_font(self, base64_str):
        """解码base64字体"""
        cache_key = base64_str[:50]  # 用前50字符作为缓存key

        if cache_key in self.font_cache:
            return self.font_cache[cache_key]

        try:
            font_data = BytesIO(base64.b64decode(base64_str))
            self.font_cache[cache_key] = font_data
            return font_data
        except Exception as e:
            print(f"Base64字体解码失败: {e}")
            return None

    def analyze_font(self, font_data):
        """分析字体文件，获取编码映射"""
        try:
            font = TTFont(font_data)

            # 获取cmap表（字符映射）
            cmap = font.getBestCmap()

            # 获取glyf表（字形）
            glyph_order = font.getGlyphOrder()

            # 构建映射关系
            mapping = {}

            # 尝试匹配字形名称与文字
            for code, glyph_name in cmap.items():
                # 十六进制转十进制
                dec_code = str(code)

                # 尝试从自定义映射表查找
                if self.custom_mapping and dec_code in self.custom_mapping:
                    mapping[chr(code)] = self.custom_mapping[dec_code]

            return mapping
        except Exception as e:
            print(f"字体分析失败: {e}")
            return {}

    def decrypt_text(self, text, mapping):
        """使用映射表解密文本"""
        if not text or not mapping:
            return text

        result = []
        for char in text:
            # 检查字符是否在映射表中
            if char in mapping:
                result.append(mapping[char])
            else:
                # 不在映射表中，保留原字符
                result.append(char)

        return ''.join(result)

    def process_html(self, html, use_custom_mapping=True):
        """处理HTML，自动解密其中的加密文字"""
        if not html:
            return html

        # 第一步：提取字体信息
        font_urls, base64_fonts = self._parse_font_from_html(html)

        # 第二步：获取字体映射
        mapping = {}

        if use_custom_mapping and self.custom_mapping:
            # 使用自定义映射表（你提供的dict_data2_xs）
            # 需要将十进制码点转换为Unicode字符
            for dec_str, char in self.custom_mapping.items():
                try:
                    unicode_code = int(dec_str)
                    # 限制在BMP范围内
                    if 0 <= unicode_code <= 0xFFFF:
                        mapping[chr(unicode_code)] = char
                except ValueError:
                    continue

        # 第三步：如果没有自定义映射，尝试从字体文件分析
        if not mapping:
            # 处理URL字体
            for font_url in font_urls[:2]:  # 只处理前两个
                font_data = self.download_font(font_url)
                if font_data:
                    url_mapping = self.analyze_font(font_data)
                    mapping.update(url_mapping)

            # 处理base64字体
            for base64_font in base64_fonts[:2]:  # 只处理前两个
                font_data = self.decode_base64_font(base64_font)
                if font_data:
                    base64_mapping = self.analyze_font(font_data)
                    mapping.update(base64_mapping)

        # 第四步：解密整个HTML
        if mapping:
            # 只解密文本部分，避免破坏HTML标签
            def replace_match(match):
                text = match.group(0)
                return self.decrypt_text(text, mapping)

            # 使用正则匹配文本节点（不在标签内）
            # 简化版：直接替换整个HTML中的加密字符
            for encrypted_char, real_char in mapping.items():
                if encrypted_char != real_char:  # 避免不必要的替换
                    html = html.replace(encrypted_char, real_char)

        return html