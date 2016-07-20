CD ./Build
CALL npm install
CALL node_modules\.bin\gulp build --configuration Debug --build-number 0
CD ..
