import sys
import json
import datetime
import pymysql
import pymysql.cursors
# from custom_exception.exceptions import CustomException 

# from models.user import UserModel
# from models.shop import ShopModel
# from models.unit_price import UnitPriceModel
# from models.collection_slip import CollectionSlipModel

sys.path.append('../')

class ConnectionFactory:

  conn = None

  @staticmethod
  def open():
    if ConnectionFactory.conn is not None:
      try:
        with ConnectionFactory.conn.cursor() as cursor:
          sql = "SELECT now()"
          cursor.execute(sql)
          result = cursor.fetchall()

          return ConnectionFactory.conn
      except:
        ConnectionFactory.conn = None

    host = 'cassini-dev-db.cpraideugqca.ap-northeast-1.rds.amazonaws.com'
    user = 'cassini'
    password = 'k(NVD$Kg288u'
    database = 'cassini'
    charset = 'utf8mb4'
    program_name = 'AWS_LAMBDA_FUNCTION_NAME'


    ConnectionFactory.conn = pymysql.connect(host=host,
                    user=user,
                    db=database,
                    password=password,
                    charset=charset,
                    program_name=program_name,
                    cursorclass=pymysql.cursors.DictCursor)

    return ConnectionFactory.conn

class UserModel:
    @staticmethod
    def getUserByLoginId(loginId):
        conn = ConnectionFactory.open()
        try:
            with conn.cursor() as cursor:
                query = "SELECT * FROM MST_USER WHERE LOGIN_ID = %s ;"
                param = (loginId,)
                cursor.execute(query, param)
                userData = cursor.fetchone()

        except Exception as e:
            raise Exception(e)

        finally:
            cursor.close()

        return userData
    
    @staticmethod
    def getUserByUserId(userId):
        conn = ConnectionFactory.open()
        try:
            with conn.cursor() as cursor:

                query = "SELECT * FROM MST_USER WHERE USER_ID = %s ;"
                param = (userId,)
                cursor.execute(query, param)
                userData = cursor.fetchone()

        except Exception as e:
            raise Exception(e)

        finally:
            cursor.close()

        return userData
    
    @staticmethod
    def updateUserName(username, userId):
        conn = ConnectionFactory.open()
        conn.begin()
        try:
            with conn.cursor() as cursor:
                query = 'UPDATE MST_USER SET USER_NAME=\'{user_name}\', LAST_UPDATED_BY=\'{user_id}\' WHERE USER_ID={user_id};'.format(
                    user_name=username, user_id=userId)
                cursor.execute(query)
                conn.commit()

                userData = UserModel.getUserByUserId(int(userId))
    
        except Exception as e:
            raise Exception(e)

        finally:
            cursor.close()

        return userData

class CollectionSlipModel:
    historyItemsQueryStr = "h.id, h.SHOP_ID, DATE_FORMAT(h.COLLECTED_DATE, '%%Y-%%m-%%d') AS COLLECTED_DATE," \
        " h.VEHICLE_ID, h.ITEM_ID, h.TEMP_QTY, h.QTY," \
        " h.PACKING_WEIGHT, h.CHECKED, h.NOTICE, h.ROUTE_ID, h.CREATED_BY, h.DELETED_BY," \
        " i.ITEM_NAME, v.VEHICLE_NAME, r.ROUTE_NAME, s.SHOP_NAME, s.SHOP_SHORT_NAME, s.ADDRESS, s.LAT, s.LON, s.CUSTOMER_ID "

    historyFromQueryStr = " FROM TBL_COLLECT_SLIP AS h " \
        " LEFT JOIN MST_ITEM AS i ON h.ITEM_ID = i.ITEM_ID " \
        " LEFT JOIN MST_VEHICLE AS v ON h.VEHICLE_ID = v.VEHICLE_ID " \
        " LEFT JOIN TBL_ROUTE AS r ON h.ROUTE_ID = r.ROUTE_ID " \
        " LEFT JOIN MST_SHOP AS s ON h.SHOP_ID = s.SHOP_ID "

    @staticmethod
    def formatDataReturn(dataList):
        dictData = {}
        _SHOP_ID = "SHOP_ID"
        _COLLECTED_DATE = "COLLECTED_DATE"
        _SEPERATE = "_"
        for data in dataList:
            shopId = data[_SHOP_ID]
            collectedDate = data[_COLLECTED_DATE]
            key = str(shopId) + _SEPERATE + str(collectedDate)
            if key in dictData.keys():
                dictData[key]['items'].append(data)
            else:
                data_shop = {'SHOP_ID': data['SHOP_ID'], 'SHOP_NAME': data['SHOP_NAME'],
                             'SHOP_SHORT_NAME': data['SHOP_SHORT_NAME'],
                             'COLLECTED_DATE': data['COLLECTED_DATE'], 'ADDRESS': data['ADDRESS'], 'LAT': data['LAT'], 'LON': data['LAT'], 'items': [data]}
                dictData[key] = data_shop

        rtData = [dictData[key] for key in dictData.keys()]

        return rtData

    @staticmethod
    def getCollectionSlipItembyId(id, userId):
        try:
            conn = ConnectionFactory.open()
            conn.begin()

            with conn.cursor() as cursor:
                query = "SELECT * FROM TBL_COLLECT_SLIP WHERE id = %s AND CREATED_BY = %s;"
                param = (str(id), userId)

                cursor.execute(query, param)
                historyData = cursor.fetchone()
            conn.commit()
            
        except Exception as e:
            raise Exception(e)

        finally:
            if cursor != None:
                cursor.close()

        return historyData

    @staticmethod
    def deleteWeighingItem(id, userId):
        try:
            conn = ConnectionFactory.open()
            conn.begin()

            with conn.cursor() as cursor:
                params = (
                    str(userId),
                    str(userId),
                    str(id)
                )

                query = 'UPDATE TBL_COLLECT_SLIP SET ' \
                        ' LAST_UPDATED_BY = %s,' \
                        ' DELETED_BY = %s,' \
                        ' DELETED_TS = CURRENT_TIMESTAMP' \
                        ' WHERE id= %s;'

                result = cursor.execute(query, params)

            conn.commit()

        except Exception as e:
            if conn != None:
                conn.rollback()
            raise Exception(e)

        finally:
            if cursor != None:
                cursor.close()

        return result

    @staticmethod
    def updateWeighingItem(historyDataParam):
        try:
            conn = ConnectionFactory.open()
            conn.begin()

            with conn.cursor() as cursor:
                query = 'UPDATE TBL_COLLECT_SLIP SET ITEM_ID = %s,' \
                    ' BUY_OR_SELL = %s,' \
                    ' UNIT = %s,' \
                    ' UNIT_PRICE = %s,' \
                    ' DISPOSAL_UNIT_PRICE = %s,' \
                    ' QTY = %s,' \
                    ' TEMP_QTY = %s,' \
                    ' PACKING_WEIGHT = %s,' \
                    ' AMOUNT = %s,' \
                    ' DISPOSAL_AMOUNT = %s,' \
                    ' NOTICE = %s,' \
                    ' LAST_UPDATED_BY = %s ' \
                    ' WHERE id= %s' \
                    ' AND ISNULL(CHECKED);'

                result = cursor.execute(query, historyDataParam)

            conn.commit()

        except Exception as e:
            if conn != None:
                conn.rollback()
            raise Exception(e)

        finally:
            if cursor != None:
                cursor.close()

        return result

    @staticmethod
    def getLastCollectedInfo(lastCollectedInfoParam):
        try:
            itemsQueryStr = "h.id, h.SHOP_ID, DATE_FORMAT(h.COLLECTED_DATE, '%%Y-%%m-%%d') AS COLLECTED_DATE," \
                            " h.TEMP_QTY, h.QTY," \
                            " h.PACKING_WEIGHT, h.CHECKED, h.NOTICE, h.CREATED_BY, h.DELETED_BY "

            fromQueryStr = " FROM TBL_COLLECT_SLIP AS h "

            condition = " WHERE h.CREATED_BY = %s AND h.TENANT_ID = %s" \
                        " AND h.SHOP_ID = %s AND h.ITEM_ID = %s " \
                        " AND ISNULL(h.DELETED_BY) " \
                        " ORDER BY h.COLLECTED_DATE DESC, h.id DESC LIMIT 1"

            query = " SELECT " + itemsQueryStr + fromQueryStr + condition

            conn = ConnectionFactory.open()
            conn.begin()

            with conn.cursor() as cursor:
                cursor.execute(query, lastCollectedInfoParam)
                lastCollectData = cursor.fetchone()
            conn.commit()
            
        except Exception as e:
            raise Exception(e)

        finally:
            if cursor != None:
                cursor.close()

        return lastCollectData

    @staticmethod
    def insertWeighingItem(payload, userData):
        try:
            shopDataList = ShopModel.getAllShops()
            print(shopDataList)

            for data in payload:
                if data["PACKING_WEIGHT"] > data["QTY"]:
                    raise CustomException(CustomException.PACKING_WEIGHT_LIMIT_VALIDATION_ERROR(
                        data=data))

                if data["PACKING_WEIGHT"] < 0 or data["QTY"] < 0 or data["TEMP_QTY"] < 0:
                    raise CustomException(CustomException.WEIGHT_PACKING_WEIGHT_QTY_NEGATIVE_VALIDATION_ERROR(
                        data=data))
                            
                shopData = ShopModel.getShopById(str(data["SHOP_ID"]))
                
                data['shopData'] = shopData
                
                if shopData is None:
                    raise CustomException(CustomException.SHOP_NOT_FOUND(data=data))
                    
                unitPriceParam = (str(userData["TENANT_ID"]), str(
                            shopData["CUSTOMER_ID"]), str(data["ITEM_ID"]))
                            
                unitData = UnitPriceModel.getUnitPrice(*unitPriceParam)
                
                if unitData is None:
                    raise CustomException(CustomException.UNIT_PRICE_NOT_FOUND(data=data))
                
                data['unitData'] = unitData
                
                amount = unitData["UNIT_PRICE"] * \
                    (data["QTY"] - data["PACKING_WEIGHT"])
                amountDisposal = unitData["DISPOSAL_UNIT_PRICE"] * \
                    (data["QTY"] - data["PACKING_WEIGHT"])
                
                data['amount'] = amount
                data['amountDisposal'] = amountDisposal
                
            conn = ConnectionFactory.open()
            conn.begin()
            
            with conn.cursor() as cursor:
                
                weighingItemParamsGen = (
                        (
                        str(userData["TENANT_ID"]),
                        str(data['shopData']["CUSTOMER_ID"]),
                        str(data['shopData']["SHOP_ID"]),
                        str(userData["COLLECTION_BASE_ID"]),
                        '\'{}\''.format(data["COLLECTED_DATE"]),
                        str(data["VEHICLE_ID"]),
                        str(data["ITEM_ID"]),
                        '\'{}\''.format(data['unitData']["BUY_OR_SELL"]),
                        '\'{}\''.format(data['unitData']["UNIT"]),
                        str(data['unitData']["UNIT_PRICE"]),
                        str(data['unitData']["DISPOSAL_UNIT_PRICE"]),
                        str(data["QTY"]),
                        str(data["TEMP_QTY"]),
                        str(data["PACKING_WEIGHT"]),
                        str(data['amount']),
                        str(data['amountDisposal']),
                        '\'{}\''.format(data["NOTICE"]),
                        str(data["ROUTE_ID"]) if data["ROUTE_ID"] else "NULL",
                        str(userData["USER_ID"]),
                        str(userData["USER_ID"]),
                        )
                        for data in payload
                        )
                
                for weighingItemParams in weighingItemParamsGen:
                    
                    query = 'INSERT INTO TBL_COLLECT_SLIP (TENANT_ID, CUSTOMER_ID, SHOP_ID, COLLECTION_BASE_ID, COLLECTED_DATE, VEHICLE_ID, ITEM_ID, BUY_OR_SELL, UNIT, UNIT_PRICE, DISPOSAL_UNIT_PRICE, QTY, TEMP_QTY, PACKING_WEIGHT, AMOUNT, DISPOSAL_AMOUNT, NOTICE, ROUTE_ID, LAST_UPDATED_BY, CREATED_BY) VALUES(%s);' % ','.join(
                        weighingItemParams)
                    
                    result = cursor.execute(query)
                
            conn.commit()

        except Exception as e:
            if conn != None:
                conn.rollback()
            raise e
        
        finally:
            if cursor != None:
                cursor.close()

        return result

    @staticmethod
    def getHistoryReference(lastUpdate, currentDateStr, userID):
        try:
            conn = ConnectionFactory.open()
            with conn.cursor() as cursor:
                query = 'SELECT DATE_SUB(%s, INTERVAL 1 MONTH) AS LAST_MONTH_DATE;'
                param = (currentDateStr,)
                cursor.execute(query, param)
                preOneMonthData = cursor.fetchone()
                preOneMonthDateStr = preOneMonthData["LAST_MONTH_DATE"]

                nowData = DbModel.getDbCurrentTime()
                newLastUpdate = iso_formatter(nowData["CURRENT_TS"])

                # conditionNewList = " WHERE h.CREATED_BY = %s " \
                #     " AND h.COLLECTED_DATE BETWEEN %s AND %s " \
                #     " AND h.CREATED_TS > %s " \
                #     " AND h.CREATED_TS <= %s;"
                #
                # conditionChangeList = " WHERE h.CREATED_BY = %s " \
                #     " AND h.COLLECTED_DATE BETWEEN %s AND %s " \
                #     " AND h.CREATED_TS <= %s " \
                #     " AND (" \
                #     " (h.LAST_UPDATED_TS > %s " \
                #     " AND h.LAST_UPDATED_TS <= %s)" \
                #     " OR (i.LAST_UPDATED_TS > %s " \
                #     " AND i.LAST_UPDATED_TS <= %s)" \
                #     " OR (v.LAST_UPDATED_TS > %s " \
                #     " AND v.LAST_UPDATED_TS <= %s)" \
                #     " OR (r.LAST_UPDATED_TS > %s " \
                #     " AND r.LAST_UPDATED_TS <= %s)" \
                #     " OR (s.LAST_UPDATED_TS > %s " \
                #     " AND s.LAST_UPDATED_TS <= %s)" \
                #     ") ;"
                

                conditionNewList = " WHERE h.CREATED_BY = %s " \
                    " AND h.COLLECTED_DATE BETWEEN %s AND %s " \
                    " AND (" \
                    " (h.LAST_UPDATED_TS > %s " \
                    " AND h.LAST_UPDATED_TS <= %s)" \
                    " OR (i.LAST_UPDATED_TS > %s " \
                    " AND i.LAST_UPDATED_TS <= %s)" \
                    " OR (v.LAST_UPDATED_TS > %s " \
                    " AND v.LAST_UPDATED_TS <= %s)" \
                    " OR (r.LAST_UPDATED_TS > %s " \
                    " AND r.LAST_UPDATED_TS <= %s)" \
                    " OR (s.LAST_UPDATED_TS > %s " \
                    " AND s.LAST_UPDATED_TS <= %s)" \
                    ") ;"

                #
                # query = "SELECT " + CollectionSlipModel.historyItemsQueryStr + \
                #     CollectionSlipModel.historyFromQueryStr + conditionNewList
                # param = (userID, preOneMonthDateStr,
                #          currentDateStr, lastUpdate, newLastUpdate)
                #
                # cursor.execute(query, param)
                # historyDataNew = cursor.fetchall()
                # #
                query = "SELECT " + CollectionSlipModel.historyItemsQueryStr + \
                    CollectionSlipModel.historyFromQueryStr + conditionNewList

                param = (userID, preOneMonthDateStr,
                         currentDateStr) + (lastUpdate, newLastUpdate) * 5

                cursor.execute(query, param)
                historyDataNew = cursor.fetchall()

                data = {
                    'historyListNew': historyDataNew,
                    # 'historyListChange': historyDataChanged,
                    'lastUpdateNew': newLastUpdate
                }

                returnValue = {
                    'result': True,
                    'data': data
                }
            conn.commit()
            
        except Exception as e:
            print("error", e)
            returnValue = {
                'result': False,
                'data': None
            }

        finally:
            if cursor != None:
                cursor.close()

        return returnValue

    @staticmethod
    def getHistoryData(userID, collectDate):
        try:
            conn = ConnectionFactory.open()
            with conn.cursor() as cursor:
                condition = " WHERE h.CREATED_BY = %s " \
                            " AND h.COLLECTED_DATE = %s " \
                            " ORDER BY SHOP_ID DESC, id DESC;"
                param = (userID, collectDate)

                query = "SELECT " + CollectionSlipModel.historyItemsQueryStr + \
                    CollectionSlipModel.historyFromQueryStr + condition

                cursor.execute(query, param)
                historyData = cursor.fetchall()

                data = {
                    'historyList': CollectionSlipModel.formatDataReturn(historyData)
                }

                returnValue = {
                    'result': True,
                    'data': data
                }
            conn.commit()
            
        except Exception as e:
            print("error", e)
            returnValue = {
                'result': False,
                'data': None
            }

        finally:
            if cursor != None:
                cursor.close()

        return returnValue

    @staticmethod
    def getHistoryDataByStoreDetail(userID, collectDate, collectShop):
        try:
            conn = ConnectionFactory.open()
            with conn.cursor() as cursor:
                condition = " WHERE h.CREATED_BY = %s " \
                    " AND h.COLLECTED_DATE = %s " \
                    " AND h.SHOP_ID = %s;"
                param = (userID, collectDate, collectShop)

                query = "SELECT " + CollectionSlipModel.historyItemsQueryStr + \
                    CollectionSlipModel.historyFromQueryStr + condition

                cursor.execute(query, param)
                historyData = cursor.fetchall()

                data = {
                    'historyList': CollectionSlipModel.formatDataReturn(historyData)
                }

                returnValue = {
                    'result': True,
                    'data': data
                }
            conn.commit()
            
        except Exception as e:
            print("error", e)
            returnValue = {
                'result': False,
                'data': None
            }

        finally:
            if cursor != None:
                cursor.close()

        return returnValue

    @staticmethod
    def getHistoryDatabyId(id):
        try:
            conn = ConnectionFactory.open()
            with conn.cursor() as cursor:
                conditionQueryStr = " WHERE h.id = %s"

                query = "SELECT " + CollectionSlipModel.historyItemsQueryStr + \
                    CollectionSlipModel.historyFromQueryStr + conditionQueryStr

                param = (id,)
                cursor.execute(query, param)
                historyData = cursor.fetchone()
            conn.commit()
            
        except Exception as e:
            raise Exception(e)

        finally:
            cursor.close()

        return historyData

class UnitPriceModel:
    @staticmethod
    def getUnitPrice(tenantId, customerId, itemId):
        conn = ConnectionFactory.open()
        conn.begin()
        try:
            with conn.cursor() as cursor:
                query = "SELECT * FROM TBL_UNIT_PRICE WHERE TENANT_ID = %s" \
                        " AND CUSTOMER_ID = %s" \
                        " AND ITEM_ID = %s;"

                param = (str(tenantId), str(customerId), str(itemId))

                cursor.execute(query, param)
                unitData = cursor.fetchone()
            conn.commit()
            
        except Exception as e:
            raise Exception(e)

        finally:
            cursor.close()

        return unitData

    @staticmethod
    def getAllUnitPrices():
        conn = ConnectionFactory.open()
        try:
            with conn.cursor() as cursor:
                query = "SELECT * FROM TBL_UNIT_PRICE;"
                cursor.execute(query)
                unitData = cursor.fetchall()

            conn.commit()
        
        except Exception as e:
            raise Exception(e)

        finally:
            cursor.close()

        return unitData

class ShopModel:
    itemsQuery = "TENANT_ID, SHOP_ID, CUSTOMER_ID, SHOP_NAME, SHOP_NAME_KANA, SHOP_SHORT_NAME, EXT_CODE," \
                " DISPLAY_PRIORITY, POSTAL_CODE, PREFECTURES, ADDRESS, MANAGER, TEL, FAX, MAIL, LAT, LON, DELETED_BY"    
    # @staticmethod
    # def getNewShop(tenantId, lastUpdate, newLastUpdate):
    #     conn = ConnectionFactory.open()
    #     try:
    #         with conn.cursor() as cursor:
    #             query = "SELECT " + ShopModel.itemsQuery + " FROM MST_SHOP " \
    #                                             " WHERE TENANT_ID = %s " \
    #                                             " AND CREATED_TS > %s " \
    #                                             " AND CREATED_TS <= %s" \
    #                                             " ORDER BY SHOP_NAME ASC;"
    #             param = (tenantId, lastUpdate, newLastUpdate)
    #             cursor.execute(query, param)
    #             shopDataNew = cursor.fetchall()
    #         conn.commit()
    #
    #     except Exception as e:
    #         raise Exception(e)
    #
    #     finally:
    #         if cursor != None:
    #             cursor.close()
    #
    #     return shopDataNew

    @staticmethod
    def getChangedShop(tenantId, lastUpdate, newLastUpdate):
        conn = ConnectionFactory.open()
        try:
            with conn.cursor() as cursor:
                query = "SELECT " + ShopModel.itemsQuery + " FROM MST_SHOP " \
                                                " WHERE TENANT_ID = %s " \
                                                " AND LAST_UPDATED_TS > %s " \
                                                " AND LAST_UPDATED_TS <= %s " \
                                                " ORDER BY SHOP_NAME ASC;"
                param = (tenantId, lastUpdate, newLastUpdate)
                cursor.execute(query, param)
                shopDataChanged = cursor.fetchall()
            conn.commit()
            
        except Exception as e:
            raise Exception(e)

        finally:
            if cursor != None:
                cursor.close()
            
        return shopDataChanged

    @staticmethod
    def getShopById(id):
        conn = ConnectionFactory.open()
        conn.begin()
        try:
            with conn.cursor() as cursor:
                query = "SELECT * FROM MST_SHOP WHERE SHOP_ID = %s;"
                param = (str(id),)
                cursor.execute(query, param)
                shopData = cursor.fetchone()
            conn.commit()
            
        except Exception as e:
            raise Exception(e)

        finally:
            if cursor != None:
                cursor.close()
            
        return shopData

    @staticmethod
    def getAllShops():
        try:
            conn = ConnectionFactory.open()
            
            with conn.cursor() as cursor:
                query = "SELECT * FROM MST_SHOP;"
                cursor.execute(query)
                shopData = cursor.fetchall()
            conn.commit()
        
        except Exception as e:
            raise Exception(e)

        finally:
            if cursor != None:
                cursor.close()
            
        return shopData

class CustomException(Exception):

    MISSING_PATH_PARAMETER = lambda data=None : json.dumps({
            "errorCode": 151,
            "message": "Missing Path Parameter",
            "data": data
    })

    
    MISSING_PAYLOAD = lambda data=None : json.dumps({
            "errorCode": 152,
            "message": "Missing Payload",
            "data": data
    })

    # User
    USER_NOT_FOUND = lambda data=None : json.dumps({
            "errorCode": 101,
            "message": "User does not exist",
            "data": data
    })

    MISSING_USERNAME = lambda data=None : json.dumps({
            "errorCode": 115,
            "message": "Missing username",
            "data": data
    })

    COLLECTION_BASE_NOT_FOUND = lambda data=None : json.dumps({
            "errorCode": 107,
            "message": "Shop does not exist",
            "data": data
    })

    MISSING_LOGIN_ID = lambda data=None : json.dumps({
            "errorCode": 108,
            "message": "Missing data for login_id",
            "data": data
    })

    MISSING_USER_ID = lambda data=None : json.dumps({
            "errorCode": 119,
            "message": "Missing data for USER_ID",
            "data": data
    })

    # Collection Slip 
    HISTORY_DATA_NOT_FOUND = lambda data=None : json.dumps({
            "errorCode": 102,
            "message": "History data does not exist",
            "data": data
    })
    
    EDIT_CONFIRMED_WEIGHING_ITEM_ERROR = lambda data=None : json.dumps({
            "errorCode": 103,
            "message": "Weighing item has been confirmed. Unable to edit item",
            "data": data
    })

    DELETE_CONFIRMED_WEIGHING_ITEM_ERROR = lambda data=None : json.dumps({
            "errorCode": 104,
            "message": "Weighing item has been confirmed. Unable to delete item",
            "data": data
    })

    PACKING_WEIGHT_LIMIT_VALIDATION_ERROR = lambda data=None : json.dumps({
            "errorCode": 109,
            "message": "Packing weight cannot be greater than weight",
            "data": data
    })

    WEIGHT_PACKING_WEIGHT_QTY_NEGATIVE_VALIDATION_ERROR = lambda data=None : json.dumps({
            "errorCode": 110,
            "message": "Packing weight, Weight and Quantity cannot be negative",
            "data": data
    })

    UPDATE_DATA_ERROR = lambda data=None : json.dumps({
            "errorCode": 111,
            "message": "Unable to update data",
            "data": data
    })    

    GET_HISTORY_DATA_ERROR = lambda data=None : json.dumps({
            "errorCode": 112,
            "message": "Unable to get history data",
            "data": data
    })     

    SUBMIT_DATA_WEIGHING_ERROR = lambda data=None : json.dumps({
            "errorCode": 113,
            "message": "Unable to submit data weighing information",
            "data": data
    }) 

    DELETE_DATA_ERROR = lambda data=None : json.dumps({
            "errorCode": 114,
            "message": "Unable to delete data",
            "data": data
    })

    MISSING_COLLECTION_SLIP_ID = lambda data=None : json.dumps({
            "errorCode": 118,
            "message": "Missing data for collection slip id",
            "data": data
    })

    # Shop
    MISSING_SHOP_ID = lambda data=None : json.dumps({
            "errorCode": 116,
            "message": "Missing data for shop_id",
            "data": data
    }) 

    SHOP_NOT_FOUND = lambda data=None : json.dumps({
            "errorCode": 106,
            "message": "Shop does not exist",
            "data": data
    })

    # Item
    MISSING_ITEM_ID = lambda data=None : json.dumps({
            "errorCode": 117,
            "message": "Missing data for item_id",
            "data": data
    })

    # Unit Price
    UNIT_PRICE_NOT_FOUND = lambda data=None : json.dumps({
            "errorCode": 105,
            "message": "Unit price does not exist",
            "data": data
    })


def handler(event: dict):

    pathParam = event.get('pathParams', None)
    if not pathParam:
        raise CustomException(CustomException.MISSING_PATH_PARAMETER())

    loginId = pathParam.get('login_id', None)
    if not loginId:
        raise CustomException(CustomException.MISSING_LOGIN_ID())

    userData = UserModel.getUserByLoginId(loginId)
    if userData is None:
        raise CustomException(CustomException.USER_NOT_FOUND())

    requestBody = event.get("body")
    payload = requestBody.get("body")
    if not payload:
        raise CustomException(CustomException.MISSING_PAYLOAD())

    CollectionSlipModel.insertWeighingItem(payload, userData)

    return json.dumps(True)


input = {
    "pathParams": {"login_id": "techvify_05"},
    "body": {"body": [
    {
      "SHOP_ID": 2147,
      "COLLECTED_DATE": "2022-01-01",
      "VEHICLE_ID": 132,
      "ITEM_ID": 452,
      "QTY": 211.25,
      "TEMP_QTY": 10,
      "PACKING_WEIGHT": 11.2,
      "NOTICE": "313 notice of item 1 15022022 - mamama Tea",
      "ROUTE_ID": 14
    },
    {
      "SHOP_ID": 2147,
      "COLLECTED_DATE": "2022-01-02",
      "VEHICLE_ID": 132,
      "ITEM_ID": 452,
      "QTY": 218.2,
      "TEMP_QTY": 10,
      "PACKING_WEIGHT": 11.2,
      "NOTICE": "notice of item 2 15022022 sharetea",
      "ROUTE_ID": 14
    },
    {
      "SHOP_ID": 2147,
      "COLLECTED_DATE": "2022-01-03",
      "VEHICLE_ID": 132,
      "ITEM_ID": 452,
      "QTY": 219.2,
      "TEMP_QTY": 10,
      "PACKING_WEIGHT": 11.2,
      "NOTICE": "Notice of item 3 15022022 tocotoco",
      "ROUTE_ID": 14
    }
  ]}
}

print(handler(input))
