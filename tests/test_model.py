import unittest
from asset_base.common import TestSession
from asset_base.entity import Currency, Domicile, Entity, Issuer

from asset_base.model import Model, ModelParameter


class TestModel(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Model
        # Test strings
        cls.currency_code = 'ZZZ'
        cls.currency_name = 'The Currency'
        cls.domicile_code = 'ZZ'
        cls.domicile_name = 'The Place'
        cls.issuer_name = 'The Corporation'
        cls.model_name = 'The Model'
        cls.ticker = 'TICK'
        cls.identity_code = 'TICK'
        cls.name_child1 = 'The Child Entity 1'
        cls.name_child2 = 'The Child Entity 2'
        cls.name_child3 = 'The Child Entity 3'
        cls.name_child4 = 'The Child Entity 4'
        cls.name_child5 = 'The Child Entity 5'
        cls.name_child6 = 'The Child Entity 6'
        cls.weight1 = 0.8
        cls.weight2 = 0.2
        cls.weight3 = 0.6
        cls.weight4 = 0.4
        cls.weight5 = 0.6
        cls.weight6 = 0.4
        cls.parameters = {
            'parameter1': '100.0',
            'parameter2': '200.0'
        }
        cls.test_str = {
            'parameter1': 'Model parameter "parameter1" = "100.0"',
            'parameter2': 'Model parameter "parameter2" = "200.0"'
        }
        cls.test_str = \
            'Model The Model (TICK) issued by The Corporation in The Place.'

    def setUp(self):
        """Set up test case fixtures."""
        self.session = TestSession().session

    def test___init__(self):
        """Instance initialization."""
        currency = Currency(self.currency_code, self.currency_name)
        domicile = Domicile(self.domicile_code, self.domicile_name,
                            currency)
        issuer = Issuer(self.issuer_name, domicile)
        # Create model parameters.
        parameters = list()
        for name, value in self.parameters.items():
            parameters.append(ModelParameter(name, value))
        # Create child entities.
        child1 = Entity(self.name_child1, domicile)
        child2 = Entity(self.name_child2, domicile)
        # Create parent entity.
        model = Model(self.model_name, issuer, self.ticker,
                      parameters=parameters)
        # Create EntityWeight instances.
        entity_weight1 = EntityWeight(model, child1, self.weight1)
        entity_weight2 = EntityWeight(model, child2, self.weight2)
        model.children_weights = [entity_weight1, entity_weight2]
        self.assertIsInstance(model, Model)
        self.assertIsInstance(entity_weight1, EntityWeight)
        self.assertIsInstance(entity_weight2, EntityWeight)
        self.session.add(model)
        self.session.commit()

    def test___str__(self):
        """Strings"""
        currency = Currency(self.currency_code, self.currency_name)
        domicile = Domicile(self.domicile_code, self.domicile_name,
                            currency)
        issuer = Issuer(self.issuer_name, domicile)
        # Create model parameters.
        parameters = list()
        for name, value in self.parameters.items():
            parameters.append(ModelParameter(name, value))
        # Create child entities.
        child1 = Entity(self.name_child1, domicile)
        child2 = Entity(self.name_child2, domicile)
        # Create parent entity.
        model = Model(self.model_name, issuer, self.ticker,
                      parameters=parameters)
        # Create EntityWeight instances.
        entity_weight1 = EntityWeight(model, child1, self.weight1)
        entity_weight2 = EntityWeight(model, child2, self.weight2)
        model.children_weights = [entity_weight1, entity_weight2]
        self.assertIsInstance(model, Model)
        self.assertIsInstance(entity_weight1, EntityWeight)
        self.assertIsInstance(entity_weight2, EntityWeight)
        self.session.add(model)
        self.session.commit()
        # Strings.
        self.assertEqual(
            model.children_weights[0].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.8000)->(The Child Entity 1 is an entity in The Place.)')
        self.assertEqual(
            model.children_weights[1].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.2000)->(The Child Entity 2 is an entity in The Place.)')

    def test_parameters(self):
        """Test parameters."""
        currency = Currency(self.currency_code, self.currency_name)
        domicile = Domicile(self.domicile_code, self.domicile_name,
                            currency)
        issuer = Issuer(self.issuer_name, domicile)
        # Create model parameters.
        parameters = list()
        for name, value in self.parameters.items():
            parameters.append(ModelParameter(name, value))
        # Create child entities.
        child1 = Entity(self.name_child1, domicile)
        child2 = Entity(self.name_child2, domicile)
        # Create parent entity.
        model = Model(self.model_name, issuer, self.ticker,
                      parameters=parameters)
        # Create EntityWeight instances.
        entity_weight1 = EntityWeight(model, child1, self.weight1)
        entity_weight2 = EntityWeight(model, child2, self.weight2)
        model.children_weights = [entity_weight1, entity_weight2]
        self.assertIsInstance(model, Model)
        self.assertIsInstance(entity_weight1, EntityWeight)
        self.assertIsInstance(entity_weight2, EntityWeight)
        self.session.add(model)
        self.session.commit()
        # Check parameters.
        parameters = model.get_parameter_dict()
        for name, value in self.parameters.items():
            self.assertEqual(parameters[name], self.parameters[name])

    def test_factory(self):
        """Factory adds children weights."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 2)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights, parameters=self.parameters)
        #  Check issuer exists.
        self.assertEqual(len(self.session.query(Issuer).all()), 1)
        #  Retrieve and reconcile.
        obj = Model.factory(self.session, self.ticker, self.issuer_name,
                            self.domicile_code, create=False)
        self.assertEqual(obj.__str__(), self.test_str)
        # Test model weights.
        weights = obj.get_weights()
        weight_dict = dict()
        for entity, value in weights:
            weight_dict[entity.id] = value
        for id, value in children_weights.items():
            self.assertEqual(weight_dict[id], value)
        # Test model parameters.
        parameters = obj.get_parameter_dict()
        for name, value in self.parameters.items():
            self.assertEqual(parameters[name], value)

    def test_factory_update(self):
        """Factory adds children weights then updates them."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        en4 = Entity.factory(self.session, self.name_child4, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 4)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        #  Check issuer exists.
        self.assertEqual(len(self.session.query(Issuer).all()), 1)
        #  Retrieve and reconcile.
        obj = Model.factory(self.session, self.ticker, self.issuer_name,
                            self.domicile_code)
        # Strings.
        self.assertEqual(
            obj.children_weights[0].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.8000)->(The Child Entity 1 is an entity in The Place.)')
        self.assertEqual(
            obj.children_weights[1].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.2000)->(The Child Entity 2 is an entity in The Place.)')
        # Changed children weight dictionary.
        children_weights = {
            en3.id: self.weight3,
            en4.id: self.weight4,
        }
        #  Retrieve and update.
        obj = Model.factory(self.session, self.ticker, self.issuer_name,
                            self.domicile_code, self.model_name,
                            weights=children_weights, update=True)
        self.session.commit()
        # Strings.
        self.assertEqual(
            obj.children_weights[0].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.6000)->(The Child Entity 3 is an entity in The Place.)')
        self.assertEqual(
            obj.children_weights[1].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.4000)->(The Child Entity 4 is an entity in The Place.)')

    def test_factory_fail_reconcile0(self):
        """The children dictionary parameter size doesn't reconcile."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 3)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        #  Retrieve and reconcile.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
            en3.id: self.weight3,
        }
        with self.assertRaises(ReconcileError) as fail:
            # Retrieve.
            Model.factory(self.session, self.ticker, self.issuer_name,
                          self.domicile_code, self.model_name,
                          weights=children_weights)
        self.assertEqual(fail.exception.message,
                         'The children:number does not reconcile.')

    def test_factory_fail_reconcile1(self):
        """The children dictionary parameter key doesn't reconcile."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        en4 = Entity.factory(self.session, self.name_child4, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 4)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        #  Retrieve and reconcile.
        children_weights = {
            en3.id: self.weight3,
            en4.id: self.weight4,
        }
        with self.assertRaises(ReconcileError) as fail:
            # Retrieve.
            Model.factory(
                self.session, self.ticker, self.issuer_name,
                self.domicile_code, self.model_name,
                weights=children_weights)
        self.assertEqual(fail.exception.message,
                         'The children:key does not reconcile.')

    def test_factory_fail_reconcile2(self):
        """The children dictionary parameter weight doesn't reconcile."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 4)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        #  Retrieve and reconcile.
        children_weights = {
            en1.id: self.weight3,
            en2.id: self.weight4,
        }
        with self.assertRaises(ReconcileError) as fail:
            # Retrieve.
            Model.factory(
                self.session, self.ticker, self.issuer_name,
                self.domicile_code, self.model_name,
                weights=children_weights)
        self.assertEqual(fail.exception.message,
                         'The weight does not reconcile.')

    def test_weights(self):
        """Compute weights of all entities in a holding. Use factory method.

        Create holding tree:

                      Parent vertice
                    /   \    Weighted edges.
          Child    1     2   Internal vertices.
                  / \   / \  Weighted edges.
          Child  3  4  5  6  Leaf vertices
        """
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add leaf children.
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        en4 = Entity.factory(self.session, self.name_child4, self.domicile_code)
        en5 = Entity.factory(self.session, self.name_child5, self.domicile_code)
        en6 = Entity.factory(self.session, self.name_child6, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 4)
        # Children weight dictionary.
        children_weights1 = {
            en3.id: self.weight3,
            en4.id: self.weight4,
        }
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code,
                             children=children_weights1)
        # Child 2.
        children_weights2 = {
            en5.id: self.weight5,
            en6.id: self.weight6,
        }
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code,
                             children=children_weights2)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add model.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        # Test
        answer = {
            'The Model': 1.00,
            'The Child Entity 1': 0.80,
            'The Child Entity 2': 0.20,
            'The Child Entity 3': 0.48,
            'The Child Entity 4': 0.32,
            'The Child Entity 5': 0.12,
            'The Child Entity 6': 0.08,
        }
        #  Get parent.
        obj = Model.factory(self.session, self.ticker, self.issuer_name,
                            self.domicile_code)
        #  All.
        result = obj.get_weights(which='all')
        for (entity, weight) in result:
            name = entity.name
            self.assertAlmostEqual(answer[name], weight, 15)
        # Leaves.
        result = obj.get_weights(which='leaves')
        for (entity, weight) in result:
            name = entity.name
            self.assertAlmostEqual(answer[name], weight, 15)


class TestModelParameter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = ModelParameter
        # Parameters
        cls.parameters = {
            'parameter1': 100.0,
            'parameter2': 200.0
        }
        cls.test_str = {
            'parameter1': 'Model parameter "parameter1" = "100.0"',
            'parameter2': 'Model parameter "parameter2" = "200.0"'
        }

    def setUp(self):
        """Set up test case fixtures."""
        self.session = TestSession().session

    def test___init__(self):
        """Instance initialization."""
        for name, value in self.parameters.items():
            parameter = ModelParameter(name, value)
            self.assertIsInstance(parameter, ModelParameter)

    def test___str__(self):
        """Strings"""
        for name, value in self.parameters.items():
            parameter = ModelParameter(name, value)
            self.assertEqual(parameter.__str__(), self.test_str[name])


