from flask import make_response, jsonify

class ApiResponse:
    api_response = None
    api_result = []
    api_message = ''
    task_error = False
    
    def __init__(self):
        self.api_result = []
        self.api_error_flag = None
        self.api_message = None

    # def add_result(self, action, api_result):
    #     self.results.append(api_result)

    # def get_results(self):
    #     return self.api_result

    # def reset(self):
    #     self.api_result = []

    def append_message(self, message):
        if self.api_message:
            self.api_message += "; " + message
        else:
            self.api_message = message

    def build_api_response(self, app_context, function):#, my_settings):
        # global my_settings, my_api_response#, api_result, api_error_flag, api_message

        with app_context:
            if self.api_error_flag:
                # if not self.api_result:
                #     self.api_result = []
                response_data = {
                    'function': function,
                    'status': 'error',
                    'message': self.api_message,
                    'result': []
                }
                api_response = make_response(jsonify(response_data), 400)
            else:
                if not self.api_result:
                    result = []
                else:
                    result = self.api_result
                if not self.api_message:
                    self.api_message = 'Completed successfully'
                response_data = {
                    'function': function,
                    'status': 'success',
                    'message': self.api_message,
                    'result': result
                }
                api_response = make_response(jsonify(response_data), 200)

            api_response.headers['Content-Type'] = 'application/json'
            # Decode with: "json.loads(api_response.get_data(as_text=True))"
            # Will be different when reading the results from another application

            self.api_response = api_response
