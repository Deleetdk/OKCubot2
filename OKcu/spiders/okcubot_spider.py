import scrapy
import csv
import json
import os
import datetime
import pdb

class OKcuSpider(scrapy.Spider):
    name = "okcubot"
    allowed_domains = ["www.okcupid.com"]
    index = 0
    max_people = 1
    domain = "https://www.okcupid.com"

    basic_info = {
                    "basics":{
                            "d_orientation":["Straight", "Gay", "Bisexual", "Asexual", "Demisexual", "Heteroflexible", "Homoflexible", "Lesbian", 
                                                                    "Pansexual", "Queer", "Questioning", "Sapiosexual"],
                            "d_gender":["Man", "Woman"],
                            "d_relationship":["Single", "Seeing someone", "Married", "In an open relationship"],
                            "d_bodytype":["Rather not say", "Thin", "Overweight", "Average build", "Fit", "Jacked", "A little", "extra", "Curvy",
                                                                    "Full figured", "Used up"]
                    },
                    "background":{
                            "d_ethnicity":["Asian", "Native American", "Indian", "Middle Eastern", "Hispanic/Latin", "White", "Black", "Pacific Islander",
                                                                    "Multi-ethnic", "Other ethnicity"],
                            "d_education_phase":["Working on", "Dropped out of", "Attended"],
                            "d_education_type":["High school", "Two-year college", "University", "space camp", "Post grad"],
                            "d_religion_type":["Agnosticism", "Atheism", "Christianty", "Judaism", "Catholicism", "Islam", "Hinduism", "Buddhism",
                                                                    "Sikh", "Jewish", "Other religion"],
                            "d_religion_seriosity":["it's important", "it's not important", "laughing about it"]
                    },
                    "misc":{
                            "d_smokes":["Never", "Sometimes", "Regularly"],
                            "d_drinks":["Often", "Socially", "Doesn"],
                            "d_drugs":["Doesn", "Sometimes", "Often"],
                            "d_offspring_current":["has kid(s)", "doesn"],
                            "d_offspring_desires":["might want", "wants", "doesn"]
                    }
            }

    personality_scale_dict = {
                #   "experienced in love"   :   "p_explove",
                    "adventurous"   :   "p_adven",
                    "indie"         :   "p_indie",
                    "spontaneous"   :   "p_spon",
                    "scientific"    :   "p_scien",
                    "independent"   :   "p_inde",
                    "confident"     :   "p_conf",
                    "mathematical"  :   "p_math",
                    "logical"       :   "p_logic",
                    "old-fashioned":    "p_oldfash",
                    "literary"      :   "p_lit",
                    "optimistic"    :   "p_opti",
                    "romantic"      :   "p_roman",
                    "compassionate":    "p_comp",
                    "love-driven"   :   "p_lovedri",
                    "spiritual"     :   "p_sprit",
                    "kinky"         :   "p_kinky",
                    "artsy"         :   "p_artsy",
                    "thrifty"       :   "p_thrift",
                    "drug-friendly":    "p_drug",
                    "arrogant"      :   "p_arro",
                    "sloppy"            :   "p_sloppy",
                    "extroverted"   :   "p_extro",
                    "geeky"         :   "p_geeky",
                #   "experienced in sex"    :   "p_expsex",
                    "capitalistic"  :   "p_capi",
                    "exercise"      :   "p_exer",
                    "kind"          :   "p_kind",
                    "pure"          :   "p_pure",
                #   "conventionally moral"          :   "p_convenmoral",
                    "mannered"      :   "p_mannered",
                    "ambitious"     :   "p_ambi",
                    "political"     :   "p_polit",
                    "greedy"            :   "p_greed",
                    "sex-driven"    :   "p_sexdrive",
                    "energetic"     :   "p_energetic",
                    "cool"          :   "p_cool",
                    "introvert"     :   "p_introvert",
                    "trusting"      :   "p_trusting",
                    "dominant"      :   "p_dominant",
                    "laid-back"     :   "p_laidback",
                    "submissive"    :   "p_submissive",
                #   "experienced in life"       :   "p_explife",
                #   "friendly to strangers"     :   "p_friendstrangers",
                    "honest"            :   "p_honest",
                    "giving"            :   "p_giving",
                    "passion"       :   "p_passion",
                    "progress"      :   "p_progress",

                    "experienced"   :   "p_exp",
            }

    def __init__(self, user, password):
        self.user = user
        self.password = password

        self.target_queue = dict()
        self.target_info_queue = []
        
        # urls which scrape users from
        self.url_list = ["https://www.okcupid.com/match?filter1=0,16&filter2=2,18,99&filter3=1,1&locid=0&timekey=1&fromWhoOnline=0&mygender=&update_prefs=1&sort_type=0&sa=1&count=100", "https://www.okcupid.com/match?filter1=0,32&filter2=2,18,99&filter3=1,1&locid=0&timekey=1&fromWhoOnline=0&mygender=&update_prefs=1&sort_type=0&sa=1&count=100"]
        #first is men, second is women

        # Patch
        self.monkey_patch_HTTPClientParser_statusReceived()

    def start_requests(self):
        self.directory = "data"
        return [scrapy.Request(self.url_list[0], callback=self.get_target, dont_filter=True)]

    # scape users in recursive way until the number of user reaches max_people
    def get_target(self, response):
        user_list = response.xpath("//div[@class='username']/a")
        
        for user in user_list:
            # if the number of users reaches max_people, start to scrape user info.
            if len(self.target_queue) == self.max_people:

                request = scrapy.FormRequest("https://www.okcupid.com/login",
                        formdata={'username': self.user, 'password': self.password},
                        callback=self.logged_in)

                yield request
                return  

            user_name = user.xpath("./text()")[0].extract()
            user_href = user.xpath("./@href")[0].extract()
            if user_name not in self.target_queue.keys():
                self.target_queue[user_name] = self.domain + user_href

        request = scrapy.Request(self.url_list[0], callback=self.get_target, dont_filter=True)
        self.index == (self.index + 1) % 3
        yield request

    # log in the site
    def logged_in(self, response):
        for user_name in self.target_queue.keys():
            print "\nStarting to scrapy %s's data.\n" % user_name
            print "Scraping basic data from %s\n" % self.target_queue[user_name]
            request = scrapy.Request(self.target_queue[user_name], callback=self.parse_user_info)   
            request.meta['user_name'] = user_name           
            yield request

    # parse target user info
    def parse_user_info(self, response):
        target_info = []
        
        target_age = response.xpath("//span[contains(@class, 'basics-asl-age')]/text()")[0].extract()
        target_info.append(self.get_info_element(response.meta["user_name"], "d_age", target_age))

        target_location = response.xpath("//span[contains(@class, 'basics-asl-location')]/text()")[0].extract()
        location = target_location.split(",")

        target_info.append(self.get_info_element(response.meta["user_name"], "d_city", location[0].strip()))
        if location[0].strip() == 'Berlin':
             target_info.append(self.get_info_element(response.meta["user_name"], "d_country", "Gemany"))
        if len(location) > 1:
            target_info.append(self.get_info_element(response.meta["user_name"], "d_city", location[1].strip()))

        target_info = self.parse_basics(response.meta["user_name"], target_info, response)
        target_info = self.parse_background(response.meta["user_name"], target_info, response)
        target_info = self.parse_misc(response.meta["user_name"], target_info, response)
        target_info = self.parse_ideal_person(response.meta["user_name"], target_info, response)
        
        # send request to go to personality page.
        url = self.target_queue[response.meta['user_name']][:-11] + "/personality"
        print "Scraping personality data from %s\n" % url
        request = scrapy.Request(url, callback=self.parse_personality)

        request.meta['user_name'] = response.meta["user_name"]
        request.meta['target_info'] = target_info

        yield request
        # print target_info

    # parse personalities of a target user
    def parse_personality(self, response):
        # get positive personalties
        positive_personalities = response.xpath("//div[contains(@class, 'chartTrait--positive')]")
        target_info = response.meta["target_info"]

        for personality in positive_personalities:
            pers_label = personality.xpath(".//div[1]/text()")[0].extract().lower().strip()
            pers_label = pers_label.split(" ")[1]

            if pers_label in self.personality_scale_dict.keys():
                pers_field = self.personality_scale_dict[pers_label]
            else:
                pers_field = "p_" + pers_label

            pers_num = personality.xpath(".//div[contains(@class, 'chartTrait-bar-inner')]/@style")[0].extract()
            pers_num = int(pers_num.split(":")[1].strip()[:-2])
            target_info.append(self.get_info_element(response.meta["user_name"], pers_field, pers_num))

        negative_personalities = response.xpath("//div[contains(@class, 'chartTrait--negative')]")

        # get negative personalties
        for personality in negative_personalities:
            pers_label = personality.xpath(".//div[1]/text()")[0].extract().lower().strip()
            pers_label = pers_label.split(" ")[1]

            if pers_label in self.personality_scale_dict.keys():
                pers_field = self.personality_scale_dict[pers_label]
            else:
                pers_field = "p_" + pers_label

            pers_num = personality.xpath(".//div[contains(@class, 'chartTrait-bar-inner')]/@style")[0].extract()
            pers_num = int(pers_num.split(":")[1].strip()[:-2]) * -1
            target_info.append(self.get_info_element(response.meta["user_name"], pers_field, pers_num))
        
        # send request to go to questions page.
        url = self.target_queue[response.meta['user_name']][:-11] + "/questions"
        request = scrapy.Request(url, callback=self.parse_questions)

        request.meta['user_name'] = response.meta["user_name"]
        request.meta['target_info'] = target_info
    
        yield request

    # parse questions of a target user
    def parse_questions(self, response):
        categories = response.xpath("//ul[@id='genre_rows']//li[@class='category']")
        urls = []
        for category in categories:
            url = self.domain + category.xpath(".//a/@href")[0].extract()
            percent = int(category.xpath(".//p/text()")[0].extract().strip().split(' ')[0][:-1])
            if percent != 0: 
                urls.append(url)

        if len(urls) > 0:
            # send request to go to questions page for each category.
            request = scrapy.Request(urls[0], callback=self.parse_questions_category)
            print "Scraping question data from " + urls[0] + '\n'
            request.meta['user_name'] = response.meta["user_name"]
            request.meta['target_info'] = response.meta['target_info']
            request.meta['index'] = 1
            request.meta['urls'] = urls
            yield request
        else:
            self.save_as_csv(response.meta["user_name"], response.meta['target_info'])
            print response.meta['target_info']
    
    # get answers according to category and page in recursive way
    def parse_questions_category(self, response):
        target_info = self.get_question_res(response.meta['user_name'], response.meta['target_info'], response)
        page_enable = response.xpath("//li[contains(@class, 'next')]/@class")[0].extract()
        
        if page_enable.find("disabled") == -1:
            page_url = response.xpath("//li[contains(@class, 'next')]/a/@href")[0].extract()
            # send request to go to questions page for each page.
            request = scrapy.Request(self.domain+page_url, callback=self.parse_questions_category)

            request.meta['user_name'] = response.meta["user_name"]
            request.meta['target_info'] = response.meta['target_info']
            request.meta['index'] = response.meta['index']
            request.meta['urls'] = response.meta['urls']
            print "Scraping question data from " + self.domain+page_url + '\n'
            yield request
            return
        else:
            if len(response.meta['urls']) > response.meta['index']:
                
                # send request to go to questions page for each category.
                request = scrapy.Request(response.meta['urls'][response.meta['index']], callback=self.parse_questions_category)
                print "Scraping question data from " + response.meta['urls'][response.meta['index']] + '\n'
                request.meta['user_name'] = response.meta["user_name"]
                request.meta['target_info'] = response.meta['target_info']
                request.meta['index'] = response.meta['index'] + 1
                request.meta['urls'] = response.meta['urls']
                yield request
            else:
                self.save_as_csv(response.meta["user_name"], target_info)
                print target_info

    # get answers in one page
    def get_question_res(self, user_name, target_info, response):
        questions = response.xpath("//div[contains(@class, 'question')]")
        for question in questions:
            answer = question.xpath(".//p[contains(@class, 'target')]//span[1]/text()")[0].extract().strip()
            temp = question.xpath("./@data-qid")
            if answer != "Answer publicly to see my answer" and len(temp) > 0:
                ques_label = "q"+question.xpath("./@data-qid")[0].extract()
                target_info.append(self.get_info_element(user_name, ques_label, answer))
        return target_info
        
    def monkey_patch_HTTPClientParser_statusReceived(self):
        """
        Monkey patch for twisted.web._newclient.HTTPClientParser.statusReceived
        """
        from twisted.web._newclient import HTTPClientParser, ParseError
        old_sr = HTTPClientParser.statusReceived
        def statusReceived(self, status):
            try:
                return old_sr(self, status)
            except ParseError, e:
                if e.args[0] == 'wrong number of parts':
                    #log.msg('Wrong number of parts in header. Assuming 200 OK', level=log.DEBUG)
                    return old_sr(self, str(status) + ' OK')
                raise
                statusReceived.__doc__ == old_sr.__doc__
        HTTPClientParser.statusReceived = statusReceived


    def get_info_element(self, user_name, field, value):
        element = dict()
    
        element['d_username'] = user_name
        element['field'] = field
        element['value'] = value

        return element;

    # get basic info about a target person like d_orientation, d_gender, d_relationship and d_bodytype
    def parse_basics(self, user_name, target_info, response):
        basics = response.xpath("//table[contains(@class, 'basics')]//td[2]/text()")[0].extract().strip().split(",")

        for basic in basics:
            basic = basic.strip()
            flag = 0
            for element in self.basic_info["basics"].keys():
                for value in self.basic_info["basics"][element]:
                    if basic.find(value) != -1:
                        target_info.append(self.get_info_element(user_name, element, value))
                        flag = 1
                        break

                if flag == 1:
                    break

        return target_info

    # get background info like d_ethnicity, d_language, d_education_phase, d_education_type, d_religion_type and d_religion_seriousity
    def parse_background(self, user_name, target_info, response):
        background = response.xpath("//table[contains(@class, 'background')]//td[2]/text()")[0].extract().strip().split(",")
        
        for bk in background:
            bk = bk.strip()
            if bk.find("Speaks") != -1:
                target_info.append(self.get_info_element(user_name, "d_languages", bk[7:])) 
                continue
            flag = 0
            for element in self.basic_info["background"].keys():
                for value in self.basic_info["background"][element]:
                    if bk.lower().find(value.lower()) != -1:
                        target_info.append(self.get_info_element(user_name, element, value))
                        if element != "d_education_phase" and element != "d_religion_type":
                            flag = 1
                        break

                if flag == 1:
                    break
        return target_info

    # get background info like d_smokes, d_drinks, d_drugs, d_offspring_current and d_offspring_desires
    def parse_misc(self, user_name, target_info, response):
        misc = response.xpath("//table[contains(@class, 'misc')]//td[2]/text()")[0].extract().strip().split(",")
        
        for ms in misc:
            ms = ms.strip()
            flag = 0
            for element in self.basic_info["misc"].keys():
                for value in self.basic_info["misc"][element]:

                    if value.lower() in ms.lower():

                        if (value == "Sometimes" or value == "Never") and ms.lower().find(element[2:-2]) == -1:
                            break 
                        
                        if value.lower() == "doesn": 
                            if element == "d_drugs" and ms.find('drugs') != -1:
                                value = "No"
                            elif element == "d_offspring_current" and ms.find('have') != -1:
                                value = "No kids"
                            elif element == "d_offspring_desires" and ms.find('want') != -1:
                                value = "Doesn't want kids"
                            else:
                                break                   
                        
                        if value.lower() == "might want" or value.lower() == "wants":
                            value += " kids"
                        target_info.append(self.get_info_element(user_name, element, value))

                        if element != "d_offspring_current":
                            flag = 1
                        break

                if flag == 1:
                    break
        
        return target_info

    # parse looking for paragraph.
    def parse_ideal_person(self, user_name, target_info, response):
        sentence = response.xpath("//div[contains(@class, 'sentence')]/text()")[0].extract().strip().split(",")
        for item in sentence:
            item = item.strip()
            if item.find('single') != -1:
                target_info.append(self.get_info_element(user_name, "lf_single", "single"))
            elif item.find('near me') != -1:
                target_info.append(self.get_info_element(user_name, "lf_location", "near me"))
            elif item.find('ages') != -1:
                age_range = item.split(' ')[1]
                min_age, max_age = age_range[:2], age_range[3:]
                
                target_info.append(self.get_info_element(user_name, "lf_min_age", min_age))
                target_info.append(self.get_info_element(user_name, "lf_max_age", max_age))
            elif item.find('for') != -1:
                target_info.append(self.get_info_element(user_name, "lf_for", item[4:-1]))

        return target_info

    def save_as_csv(self, user_name, target_info):
        
        #if folder does not exist, create it
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        path = self.directory + "/" + datetime.datetime.today().strftime("%Y-%m-%d") + ".csv"
        
        #open file for writing
        with open(path, 'a') as f:
                        #write each datapoint to the file at  the end
                        for element in target_info:
                            #make line
                            temp = '"%s","%s","%s"\n' % (element["d_username"], element["field"], element["value"])

                            #unicode encoding
                            temp = temp.encode('utf8')

                            #write line
                            f.write(temp)
