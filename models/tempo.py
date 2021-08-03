

class Tempo:

    def __init__(self: object, ano: int, isleap: bool) -> None:
        self.__isleap: bool = self.isleap()
        self.__ano: int = ano


    @property
    def ano(self: object) -> int:
        """
        Getter para a propriedade ano
        """
        return self.__ano

    def print_mes(self: object, mes):
        """
        Retorna uma lista com todos os dias do mes, para um determinado mes e ano

        """
        pass

    def print_semana(self: object, value: str):
        """
        Retorna o dia da semana em que caiu determinada data
        :param value: informado como uma string 'ddmmaa'
        :return: retorna uma string informando em qual dia da semana caiu aquela data
        """
        pass

    def isleap(self: object):
        """
        determina se um ano é bissexto
        :return: retorna bool informando se o ano é bissesto ou não
        """
        if self.ano % 4 == 0:
            return True
        else:
            return False


