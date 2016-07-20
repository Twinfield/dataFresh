CD ./Build
CALL npm install
CALL node_modules\.bin\gulp package --configuration Release --build-number 0
