import org.scalatest._


class PhoneBookTest extends FlatSpec with Matchers {

  /**
   * phonebook is the name of a test package
   * have a owner if the name of a case test inside the package phonebook
   */
  "phonebook" should "have a owner" in {

    //This code check that a IllegalArgumentException is thrown by the call new PhoneBoook(owner = null)
    the[IllegalArgumentException] thrownBy {
      new PhoneBoook(owner = null)
    } should have message "requirement failed: Phonebook owner cannot be null"
  }

  /**
   * it reference to last test package, phonebook
   * add new contact is the name of a case test inside phonebook
   */
  it should "add new contact" in {
    val book = new PhoneBoook(owner = new User(name = "igleson"))

    book.contacts.isEmpty should be equals true

    val tales = new User("tales")
    book addContact tales
    book.contacts.size should be equals 1
    book.contacts.head should be equals tales
  }

  /**
   * it reference to last test package, phonebook
   * not add repeated contacts is the name of a case test inside phonebook
   */
  it should "not add repeated contacts" in {
    val book = new PhoneBoook(owner = new User(name = "igleson"))

    book.contacts.isEmpty should be equals true

    val tales = new User("tales")
    book addContact tales
    book.contacts.size should be equals 1
    book.contacts.head should be equals tales

    the[IllegalArgumentException] thrownBy {
      book addContact tales
    } should have message "requirement failed: Cannot add repeated contacts"

    book.contacts.size should be equals 1
    book.contacts.head should be equals tales
  }

  /**
   * it reference to last test package, phonebook
   * not add null contacts is the name of a case test inside phonebook
   */
  it should "not add null contacts" in {
    val book = new PhoneBoook(owner = new User(name = "igleson"))

    book.contacts.isEmpty should be equals true

    val tales = new User("tales")
    book addContact tales
    book.contacts.size should be equals 1
    book.contacts.head should be equals tales

    the[IllegalArgumentException] thrownBy {
      book addContact null
    } should have message "requirement failed: Contact cannot be null"

    book.contacts.size should be equals 1
    book.contacts.head should be equals tales
  }

  it should "add phone number to contact" in {
    val book = new PhoneBoook(owner = new User(name = "igleson"))

    val tales = new User("tales")
    val talesNumber = 12345678

    book addContact tales

    book.addPhoneNumber("tales", talesNumber)

    tales.phones contains new Phone(talesNumber) should be equals true
  }

  it should "not add repeated phone number to contact" in {
    val book = new PhoneBoook(owner = new User(name = "igleson"))

    val tales = new User("tales")
    val talesNumber = 12345678

    book addContact tales

    book.addPhoneNumber("tales", talesNumber)

    the[IllegalArgumentException] thrownBy {
      book.addPhoneNumber("tales", talesNumber)
    } should have message "requirement failed: Cannot add repeated number to a contact"

    tales.phones contains new Phone(talesNumber) should be equals true
    tales.phones.size should be equals 1
  }

  it should "not add phone number to non existent contact" in {
    val book = new PhoneBoook(owner = new User(name = "igleson"))

    val tales = new User("tales")
    val talesNumber = 12345678

    book addContact tales

    the[RuntimeException] thrownBy {
      book.addPhoneNumber("andryw", talesNumber)
    } should have message "There is no user named andryw"
  }
}
