class CacheFunctionalitiesInterface:
    def connect(self) -> bool:
        """
        Function that handles the connection with the caching database.
        Returns:
            bool: Returns true/false based on the result of the process.
        """
        pass
    
    def set(self, key: str, value) -> bool:
        """
        Sets a KV pair in the database.
        Args:
            key (str): Key of the value
            value (any): The value of the key

        Returns:
            bool: Returns true/false based on the result of the process.
        """
        pass
    
    def get(self, key: str) -> str:
        """
        Get the value of the stored key.
        Args:
            key (str): Key of the value

        Returns:
            str: Returns the value that the key refers to in string format.
        """
        pass
    
    def flush() -> bool:
        """
        Flushes the entire store of the database.
        Returns:
            bool: Returns true/false based on the result of the process.
        """
        pass